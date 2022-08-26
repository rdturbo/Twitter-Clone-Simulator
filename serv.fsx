#r "nuget: Akka.FSharp" 
#r "nuget: Akka.Remote"
open System
open System.Text
open System.Collections.Generic
open System.Security.Cryptography
open Akka.Actor
open Akka.Configuration
open Akka.FSharp


let rnd = Random()
let configuration = 
    ConfigurationFactory.ParseString(
        @"akka {
            actor {
                provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
                debug : {
                    receive : on
                    autoreceive : on
                    lifecycle : on
                    event-stream : on
                    unhandled : on
                }
            }
            remote {
                helios.tcp {
                    port = 9002
                    hostname = localhost
                }
            }
        }")


let system = ActorSystem.Create("TwitterEngine", configuration)

let mutable clientDict = new Dictionary<string, IActorRef>()
let mutable users = new Dictionary<string, string>()
let mutable passwords = new Dictionary<string, string>()
let mutable onlineUsers = new Dictionary<string, IActorRef>()
let mutable followers = new Dictionary<string, List<string>>()
let mutable subscribersTweets = new Dictionary<string, List<string>>()
let mutable tweets = new Dictionary<string, string>()
let mutable usersTweets = new Dictionary<string, List<string>>()
let mutable mentions = new Dictionary<string, Dictionary<string, List<string>>>()
let mutable hashTags = Map.empty
let mutable tweetIDList = []

let rsa = new RSACryptoServiceProvider(2048)
let random' = Random()
let mutable seed = RandomNumberGenerator.GetInt32(1000)
let ticksPerMicroSecond = (TimeSpan.TicksPerMillisecond |> float )/(1000.0)
let ticksPerMilliSecond = TimeSpan.TicksPerMillisecond |> float

type RegisterMsg =
    | RegisterUser of string * string
    | Login of string * string
    | Logout of string
    | PublicKeyRequest of IActorRef
    | Parse of string*string*IActorRef
    | Add of string*string*string
    | Update of string*string
    | AddTweet of string*string*IActorRef
    | ReTweet of string*IActorRef
    | GetTweets of string*IActorRef
    | GetTags of string*IActorRef
    | GetHashTags of string * string * IActorRef

let byteConvertToString x = 
    BitConverter.ToString(x)

let stringConvertToByte (str: string) = 
    System.Text.Encoding.ASCII.GetBytes(str)

let hashToSHA256 (str: string) = 
    let bitArrayOfStr = stringConvertToByte str  
    let hashOutput = HashAlgorithm.Create("SHA256").ComputeHash bitArrayOfStr
    let hashStr = byteConvertToString hashOutput
    hashStr

let registerUser username password = 
    seed <- seed + RandomNumberGenerator.GetInt32(10)
    let userID = seed |> string
    users.Add(username, seed |> string)
    passwords.Add(username, password)
    userID

let getUserID username =
    if users.ContainsKey username then
        users.Item username
    else
        "User Doesn't Exist"

let getPassword username =
    if passwords.ContainsKey username then
        passwords.Item username
    else
        "Password Doesn't Exist"

let checkLogin username password = 
    let mutable userid = getUserID username 
    let mutable uid = ""
    if userid = "User Doesn't Exist" then
        uid <- registerUser username password
    else
        uid <- userid
    let mutable passwordOnRecord = getPassword username 
    if passwordOnRecord = "Password Doesn't Exist" then
        printfn "Error: corrupted passwords. Restart app."
    if onlineUsers.ContainsKey uid then
        "User Already Logged In"
    else
        onlineUsers.Add(uid, clientDict.Item(username))
        "logged in"

let checkLogout username = 
    let mutable userid = getUserID username
    if userid <> "User Doesn't Exist" then
        
        if onlineUsers.ContainsKey userid then
            onlineUsers.Remove(userid) |> ignore
            "User Logged Out"
        else
            "User Already Logged Out"
    else
        "User Doesn't Exist"        

let isOnlineUser userid = 
    let res = onlineUsers.ContainsKey userid
    res

let addTweetToUser userID tweet =
    let find = usersTweets.ContainsKey userID
    if find then    
        usersTweets.Item(userID).Add(tweet)
    else
        let tweetList = new List<string>()
        tweetList.Add(tweet)
        usersTweets.Add(userID, tweetList)

let addFollower username followerUsername =
    let userid = getUserID username
    if userid <> "User Doesn't Exist" then
        if isOnlineUser userid then
            let fid = getUserID followerUsername
            if fid <> "User Doesn't Exist" && userid <> "User Doesn't Exist" then
                
                if followers.ContainsKey(userid) then
                    followers.Item(userid).Add(fid) 
                else
                    let mutable followersList = new List<string>()
                    followersList.Add(fid) 
                    followers.Add(userid, followersList)
                let responseString = "SUBSCRIBED|"
                clientDict.Item(username) <! responseString
            else
                clientDict.Item(username) <! "NOTLOGGEDIN|"
        else
            printfn "User Not Logged in"

let addMentions userid tweet mentionusername =
    let taggedid = getUserID mentionusername
    if taggedid <> "User Doesn't Exist" then
        let mention = mentions.ContainsKey taggedid
        if mention = false then
            let mutable mp = new Dictionary<string, List<string>>()
            let mutable tlist = new List<string>()
            tlist.Add(tweet)
            mp.Add(userid,tlist)
            mentions.Add(taggedid,mp)
        else
            if mentions.Item(taggedid).ContainsKey userid then
                mentions.Item(taggedid).Item(userid).Add(tweet)
            else 
                let twtList = new List<string>()
                twtList.Add(tweet)
                mentions.Item(taggedid).Add(userid, twtList)

let addHashTag hashtag tweet =
    let hfind = hashTags.TryFind hashtag
    if hfind = None then
        let tlist = new List<string>()
        tlist.Add(tweet)
        hashTags <- hashTags.Add(hashtag,tlist)
    else
        hfind.Value.Add(tweet)
    
      

let addSubscribersTweet tweet ownerid =
    if followers.ContainsKey ownerid then
        for i in followers.Item(ownerid) do
            if subscribersTweets.ContainsKey i then
                subscribersTweets.Item(i).Add(tweet)
            else 
                let subTweetList = new List<string>()
                subTweetList.Add(tweet)
                subscribersTweets.Add(i, subTweetList)    

let getHashTags hashtag =
    let hashtagList = hashTags.TryFind hashtag
    let result = new List<string>()
    if hashtagList <> None then 
        for i in hashtagList.Value do
            result.Add(i)
    result

let concatList (list: List<string>) =
    let mutable resp = ""
    let len = Math.Min(100,list.Count)
    for i in 0 .. len-1 do
        resp <- resp+list.[i]+"|"
    resp

let randomStr = 
            let chars = "abcdefghijklmnopqrstuvwxyz0123456789"
            let charsLen = chars.Length
            fun len -> 
                let randomChars = [|for i in 0..len -> chars.[random'.Next(charsLen)]|]
                String(randomChars)

type PrintMessages =
    | HashTagsTimer of float * float * int
    | MentionsTimer of float * float * int
    | GetTweetsTimer of float * float * int
    | TweetCount of int
    | Print of int


let StatsPrinter (mailbox:Actor<_>) = 
    let mutable totalTweets = 0
    let mutable avgGetTweets = 0.0
    let mutable countGetTweets = 0
    let mutable totalGetTweets = 0.0
    let mutable avgMention = 0.0
    let mutable countMention = 0
    let mutable totalMention = 0.0
    let mutable avgHashTag = 0.0
    let mutable countHashTag = 0
    let mutable totalHashTag = 0.0
    let mutable count = 0
    let rec loop() = actor{
        
        let! msg = mailbox.Receive()
        try
            match msg with
            | TweetCount(tweetcount) ->
                totalTweets <- tweetcount
            | GetTweetsTimer(avgGT, totalGT, countGT) ->
                avgGetTweets <- avgGT
                totalGetTweets <- totalGT
                countGetTweets <- countGT
            | HashTagsTimer(avgH, totalH, countH) ->
                avgHashTag <- avgH
                totalHashTag <- totalH
                countHashTag <- countH
            | MentionsTimer(avgM, totalM, countM) ->
                avgMention <- avgM
                totalMention <- totalM
                countMention <- countM    
            | Print(printCount) ->
                              
                printfn "--------Server Statistics (%i)--------" printCount
                printfn "Total Number of Tweets             :   %i tweets" totalTweets
                printfn "Total Online Users Currently       :   %i users" onlineUsers.Count   
                printfn "GetTweets Query Requests Count     :   %i requests" countGetTweets
                printfn "GetTweets Avg. Processing Time     :   %A microseconds" avgGetTweets
                printfn "GetTweets Total Processing Time    :   %A milliseconds" totalGetTweets
                printfn "Mentions Query Requests Count      :   %i requests" countMention
                printfn "Mentions Avg. Processing Time      :   %A microseconds" avgMention
                printfn "Mentions Total Processing Time     :   %A milliseconds" totalMention
                printfn "Hashtag Query Requests Count       :   %i requests" countHashTag
                printfn "Hashtag Avg. Processing Time       :   %A microseconds" avgHashTag
                printfn "Hashtag Total Processing Time      :   %A milliseconds" totalHashTag
                printfn "--------------------------------------"
                
                count <- printCount
                system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(1000.0), mailbox.Self, Print(count+1))
              
                    
            
        finally
            let err = "ignore"
            err |> ignore                                     
        return! loop()
    }
    loop()

let statsref = spawn system "Printer" StatsPrinter




let RegistrationHandler (mailbox:Actor<_>)=
    let mutable responseString = ""
    let mutable onlineStatus = ""
    let mutable onlineCount = 0
    let rec loop() = actor{
        let! msg = mailbox.Receive()
        
        try
            match msg with 
            | PublicKeyRequest(clientSender) ->
                let pubKey = rsa.ExportParameters(false)
                let publicKey = rsa.ToXmlString(false)
                responseString <- "PublicKey|" + publicKey
                clientSender <! responseString
            | RegisterUser(username, password) ->     
                let userRegID = getUserID username
                let encryptedPasswordStr = password
                let encryptedPassword = System.Convert.FromBase64String(encryptedPasswordStr)
                let passwordBytes = rsa.Decrypt(encryptedPassword, false)
                let passwordStr = Encoding.UTF8.GetString(passwordBytes)
                let hashedPassword = hashToSHA256 passwordStr
                let mutable finalUserID = ""
                if userRegID = "User Doesn't Exist" then
                    finalUserID <- registerUser username hashedPassword
                else
                    finalUserID <- userRegID
                responseString <- ("REGISTERED|"+ finalUserID) 
                 
                clientDict.Item(username) <! responseString
            | Login(username, password) ->
                let encryptedPasswordStr = password
                let encryptedPassword = System.Convert.FromBase64String(encryptedPasswordStr)
                let passwordBytes = rsa.Decrypt(encryptedPassword, false)
                let passwordStr = Encoding.UTF8.GetString(passwordBytes)
                let hashedPassword = hashToSHA256 passwordStr
                onlineStatus <- checkLogin username hashedPassword
                // printfn "%s, %s is logged in" username passwordStr 
                // if onlineStatus = "logged in" then
                //     printfn "%s is logged in" username 
            | Logout(username) ->   
                onlineStatus <- checkLogout username
                // if onlineStatus = "User Logged Out" then
                //     // printfn "%s is logged out" username
                //     // onlineCount <- onlineUsers.Count
                //     // printfn "No. of online users = %i" onlineCount 


        finally
            let err = "Ignore"
            err |> ignore
        return! loop()
    }
    loop()

let accountActorRef = spawn system "registrationHandler" RegistrationHandler



let Parser (mailbox:Actor<_>) = 
    let rec loop() = actor{
        let! msg = mailbox.Receive()
        try
            match msg with 
            | Parse(userid,tweet,clientActor) ->    
                let tweetParseList = (tweet).Split ' '
                for i in tweetParseList do
                    if i.StartsWith "@" then
                        let mentionList = i.Split '@'
                        addMentions userid tweet mentionList.[1]
                    elif i.StartsWith "#" then
                        addHashTag i tweet
                clientActor <! "SendToClient|Tweet|"
        finally 
            let a = "ignore"
            a |> ignore
        return! loop()
    }
    loop()

let tweetParserRef = spawn system "Parser" Parser


let SubscribeActor (mailbox:Actor<_>) = 
    
    let rec loop() = actor{
        
        let! msg = mailbox.Receive()
        try
            match msg with
            | Add(user, follower, userRank) ->     
                addFollower user follower
            | Update(ownerid, tweet) ->
                addSubscribersTweet tweet ownerid    
                    
            
        finally
            let err = "ignore"
            err |> ignore                                     
        return! loop()
    }
    loop()

let subscribeActorRef = spawn system "Subscriber" SubscribeActor
    

let TweetHandler (mailbox:Actor<_>) =
    let mutable count = 0
    let mutable randomTweetID = ""
    let timer = System.Diagnostics.Stopwatch()
    let rec loop() = actor{
        let! msg = mailbox.Receive()
        
        try
            match msg with
            | AddTweet(username,tweet,clientActor) -> 
                timer.Restart()
                let userid = getUserID username
                let mutable posID = randomStr(10)
                count <- count + 1
                if userid <> "User Doesn't Exist" then
                    if isOnlineUser userid then
                        
                        while tweets.ContainsKey posID do
                            posID <- randomStr(10)
                        let tweetid = posID
                        tweetIDList <- tweetid :: tweetIDList   
                        tweets.Add(tweetid,tweet)
                        addTweetToUser userid tweet
                        tweetParserRef <! Parse(userid, tweet, clientActor)
                        subscribeActorRef <! Update(userid, tweet)
                        
                    else
                        clientActor <! "SendToClient|NotLoggedIn|"
                
                if count % 5000 = 0 then
                    statsref <! TweetCount(count)
            | ReTweet(username, clientActor) -> 
                randomTweetID <- tweetIDList.[random'.Next(tweetIDList.Length)]
                if tweets.ContainsKey randomTweetID then 
                    let retweet = tweets.Item(randomTweetID)  
                    mailbox.Self <! AddTweet(username,retweet,clientActor)                                                 
            
        finally
            let a = "Ignore "
            a |> ignore
                    

        return! loop()
    }
    loop()

let tweetActorRef = spawn system "tweetHandler" TweetHandler


let FetchActor(mailbox:Actor<_>)=
    let timer = Diagnostics.Stopwatch()
    let mutable gettweetstime = 0
    let mutable gettweetscount =0
    let mutable gettagstime = 0
    let mutable gettagscount =0
    let mutable gethashtagstime = 0
    let mutable gethashtagscount =0
    let mutable avggettweetsime = 0.0
    let mutable totalgettweetstime = 0.0
    let mutable avgmentiontime = 0.0
    let mutable totalmentiontime = 0.0
    let mutable avghashtagtime = 0.0
    let mutable totalhashtagtime = 0.0
    let rec loop()=actor{
        let! msg = mailbox.Receive()
        try
            match msg with
            | GetTweets(username,clientActor) ->    
                timer.Restart()
                gettweetscount <- gettweetscount + 1
                let userid = getUserID username
                if userid <> "User Doesn't Exist" then
                    if onlineUsers.ContainsKey userid then
                        if usersTweets.ContainsKey userid then   
                            let resp = usersTweets.Item(userid)
                            clientActor <! "ResponseFromServer|GetTweets|"+(concatList resp)+"\n"
                    else
                        clientActor <! "SendToClient|NotLoggedIn|"

                gettweetstime <- gettweetstime + (timer.ElapsedTicks|>int)
                
                if gettweetscount%100 = 0 then
                    avggettweetsime <- ((gettweetstime|>float)/(ticksPerMicroSecond*(gettweetscount|>float)))
                    totalgettweetstime <- ((gettweetstime|>float)/ticksPerMilliSecond)
                    statsref <! GetTweetsTimer(avggettweetsime, totalgettweetstime, gettweetscount)
                    
            | GetTags(username,clientActor)->   
                timer.Restart()
                gettagscount <- gettagscount + 1
                let userid = getUserID username
                let resp = new List<string>()
                if userid <> "User Doesn't Exist" then
                    if onlineUsers.ContainsKey userid then 
                        if mentions.ContainsKey userid then 
                            for i in mentions.Item(userid) do
                                for j in mentions.Item(userid).Item(i.Key) do
                                    resp.Add(j)
                        clientActor <! "ResponseFromServer|GetMentions|"+(concatList resp)+"\n"
                    else
                        clientActor <! "SendToClient|NotLoggedIn|"
                gettagstime <- gettagstime + (timer.ElapsedTicks|>int)
                if gettagscount%100 = 0 then
                    avgmentiontime <- ((gettagstime|>float)/(ticksPerMicroSecond*(gettagscount|>float)))
                    totalmentiontime <- ((gettagstime|>float)/ticksPerMilliSecond)
                    statsref <! MentionsTimer(avgmentiontime, totalmentiontime, gettagscount)
                    
            | GetHashTags(username,hashtag,clientActor)->   
                timer.Restart()
                gethashtagscount <- gethashtagscount + 1
                let resp = new List<string>()
                let userid = getUserID username
                if userid <> "User Doesn't Exist" then
                    if onlineUsers.ContainsKey userid then
                        let resp = (getHashTags hashtag)
                        clientActor <! "ResponseFromServer|GetHashTags|"+(concatList resp)+"\n"

                    else
                        clientActor <! "SendToClient|NotLoggedIn|"
                gethashtagstime <- gethashtagstime + (timer.ElapsedTicks|>int)
                if gethashtagscount%100 = 0 then
                    avghashtagtime <- (gethashtagstime|>float)/(ticksPerMicroSecond*(gethashtagscount|>float))
                    totalhashtagtime <- ((gethashtagstime|>float)/ticksPerMilliSecond)
                    statsref <! HashTagsTimer(avghashtagtime, totalhashtagtime, gethashtagscount)
                    
        with
            | :? System.InvalidOperationException as ex -> 
                let err = "ignore"                                            
                err |>ignore


        return! loop()
    }
    loop()

let fetchHandlerRef = spawn system "FetchActor" FetchActor





let commlink = 
    spawn system "RequestHandler"
    <| fun mailbox ->
        let mutable reqid = 0
        let mutable ticks = 0L
        let timer = Diagnostics.Stopwatch()
        let mutable clientSender = null
        let rec loop() =
            actor {
                
                let! msg = mailbox.Receive()
                reqid <- reqid + 1
                timer.Restart()
                
                clientSender <- mailbox.Sender()
                let request = (msg|>string).Split '|'
                if request.[0].CompareTo("SendPublicKey") = 0 then
                    accountActorRef <! PublicKeyRequest(clientSender)
                elif request.[0].CompareTo("Register") = 0 then
                    clientDict.Add(request.[1], clientSender)
                    accountActorRef <! RegisterUser(request.[1], request.[2])
                elif request.[0].CompareTo("Login") = 0 then
                    accountActorRef <! Login(request.[1], request.[2])
                elif request.[0].CompareTo("Subscribe") = 0 then 
                    subscribeActorRef <! Add(request.[1],request.[2],request.[3])
                elif request.[0].CompareTo("Logout") = 0 then
                    accountActorRef <! Logout(request.[1])
                elif request.[0].CompareTo("Tweet") = 0 then
                    tweetActorRef <! AddTweet(request.[1],request.[2],clientSender)
                elif request.[0].CompareTo("ReTweet") = 0 then
                    tweetActorRef <! ReTweet(request.[1],clientSender)
                elif request.[0].CompareTo("FetchTweets") = 0 then
                    fetchHandlerRef <! GetTweets(request.[1],clientSender)
                elif request.[0].CompareTo("FetchMentions") = 0 then
                    fetchHandlerRef <! GetTags(request.[1],clientSender)
                elif request.[0].CompareTo("FetchHashTags") = 0 then
                    fetchHandlerRef <! GetHashTags(request.[1],request.[2],clientSender)                  
                return! loop() 
            }
            
        loop()

statsref <! Print(1)

system.WhenTerminated.Wait()