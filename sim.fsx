#time "on"
#r "nuget: Akka.FSharp" 
#r "nuget: Akka.Remote"
open System
open System.Security.Cryptography
open Akka.Actor
open Akka.Configuration
open Akka.FSharp



let random = Random()
let serverip = fsi.CommandLineArgs.[1] |> string
let serverport = fsi.CommandLineArgs.[2] |>string
let N = fsi.CommandLineArgs.[3] |> int



//"akka.tcp://RemoteFSharp@localhost:8777/user/server"
let addr = "akka.tcp://TwitterEngine@" + serverip + ":" + serverport + "/user/RequestHandler"

let configuration = 
    ConfigurationFactory.ParseString(
        @"akka {
            actor {
                provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
                
            }
            remote {
                helios.tcp {
                    port = 9001
                    hostname = localhost
                }
            }
        }")

let system = ActorSystem.Create("TwitterClient", configuration)
let twitterEngine = system.ActorSelection(addr)
// let N = 100
type SimulatorMsg =
    | Initialize of int
    | RegistrationBegin of int
    | SubscribtionBegin of int

let mutable workersList = []
let tweetBeg = [|"Hey, how are you";"I am the best";"Hello";"UF is the best";"When are you visiting Gainesville?";"GoodBye";"Fsharp is Life";"Fsharp is love";"Never Die, never begin";"what's up";|]
let hashtags = [|"#rupayan";"#superbowl";"#COP5615isTheBest";"#UF";"#Gainesville";"#ActorModel";"#ToyStory";"#twitterClone";"#Winter";"#HarryPotter";|]

let randomStr = 
            let chars = "abcdefghijklmnopqrstuvwxyz0123456789"
            let charsLen = chars.Length
            fun len -> 
                let randomChars = [|for i in 0..len -> chars.[random.Next(charsLen)]|]
                String(randomChars)

let addSubscriber level (workers: list<IActorRef>) =
    let divisor = (ceil ((level - 1 + 2) |> float)) |> int 
    for i in 0 .. (N/divisor) do
        twitterEngine <! "Subscribe|"+workers.[level-1].Path.Name+"|"+workers.[i].Path.Name+"|"+(string level)

let sendTweetToServer (clientActor: IActorRef) password level (workers: list<IActorRef>) login= 
    
    let mutable actorStatus = login
    let mutable tweetString = ""

    let mutable retweet = false
    let randomVariable = RandomNumberGenerator.GetInt32(100)
    // printfn "%i" randomVariable
    let mutable vari = randomVariable
    if randomVariable%50 = 25 then
        if login && randomVariable%15 = 0 then
            actorStatus <- false
            twitterEngine <! "Logout|"+clientActor.Path.Name+"|"
        else
            actorStatus <- true
            twitterEngine <! "Login|"+clientActor.Path.Name+"|"+ password
    elif randomVariable>= 31 && randomVariable<= 33 then
        twitterEngine <! "FetchTweets|"+clientActor.Path.Name+"|"   
    elif randomVariable>= 34 && randomVariable<= 36 then
        twitterEngine <! "FetchMentions|"+clientActor.Path.Name+"|"   
    elif randomVariable>= 37 && randomVariable<= 39 then
        twitterEngine <! "FetchHashTags|"+clientActor.Path.Name+"|"+hashtags.[RandomNumberGenerator.GetInt32(hashtags.Length)]+"|"          
    else
        if randomVariable%10 =0 then
            retweet <- true
        if retweet then
            let sendTweet = "ReTweet|"+clientActor.Path.Name+"|"
            twitterEngine <! sendTweet
            retweet <- true
        else
            let randomString = randomStr(5)
            tweetString <- sprintf "Tweet|%s|%s @%s %s" clientActor.Path.Name tweetBeg.[RandomNumberGenerator.GetInt32(tweetBeg.Length)] workers.[RandomNumberGenerator.GetInt32(workers.Length)].Path.Name hashtags.[RandomNumberGenerator.GetInt32(hashtags.Length)]
            let sendTweet = tweetString   
            twitterEngine <! sendTweet
        
    let timeInterval = (level)|>int
    system.Scheduler.ScheduleTellOnce(timeInterval, clientActor, "SendRequestToServer", clientActor)
    actorStatus

let RsaEncrypt (data : byte[]) (key : RSAParameters) (padding : bool) : byte[] =
    try
        let rsa = new RSACryptoServiceProvider()
        rsa.ImportParameters(key)
        rsa.Encrypt(data, padding)
    with
    | :? CryptographicException as e ->
        printfn "%A" e.Message
        Array.empty




    

let Client (mailbox:Actor<_>)=
    
    let mutable login = true
    let mutable level = 0
    let mutable clientUsername = ""
    let mutable clientPassword = ""
    let mutable encryptedClientPassword = ""
    let mutable publicKey = ""
    let mutable dispatcherRef = null
    let rec loop() = actor{
        clientUsername <- mailbox.Self.Path.Name
        clientPassword <- randomStr(10) + "<|>" + clientUsername
        let! msg = mailbox.Receive()
        // printRef <! msg 
        let response =msg|>string
        
        let command = (response).Split '|'
        if command.[0].CompareTo("Initialization") = 0 then 
            level <- command.[1] |> int
            twitterEngine<!"SendPublicKey|" + "Please!"
            dispatcherRef <- mailbox.Sender()
        elif command.[0].CompareTo("PublicKey") = 0 then
            publicKey <- command.[1]
            let rsa = new RSACryptoServiceProvider()
            rsa.FromXmlString(publicKey)
            let clientPasswordBitArray = System.Text.Encoding.UTF8.GetBytes(clientPassword)
            let encryptedPassword = rsa.Encrypt(clientPasswordBitArray, false)
            let encryptedPasswordStr = System.Convert.ToBase64String(encryptedPassword)
            encryptedClientPassword <- encryptedPasswordStr
            // printfn "%s" encryptedPasswordStr
            twitterEngine<!"Register|" + clientUsername + "|" + encryptedPasswordStr
            twitterEngine<!"Login|" + clientUsername + "|" + encryptedPasswordStr    
        elif command.[0].CompareTo("REGISTERED") = 0 then
            // printfn "%i REGISTERED" level
            dispatcherRef <! RegistrationBegin(level)
        elif command.[0].CompareTo("StartSubscription") = 0 then
            addSubscriber level workersList 
        elif command.[0].CompareTo("SUBSCRIBED") = 0 then
            // printfn "%i REGISTERED" level
            dispatcherRef <! SubscribtionBegin(level)  
        elif command.[0].CompareTo("SendRequestToServer") = 0 then
            login <- sendTweetToServer mailbox.Self encryptedClientPassword level workersList login
        elif command.[0].CompareTo("ResponseFromServer") = 0 then
            printfn "%s \n" command.[1]
            printfn "%A \n \n" msg    
          
          
            
        return! loop()
    }
    loop()

workersList <- [for a in 1 .. N do yield(spawn system ("Client-" + (a |> string)) Client)]


let Dispatcher (mailbox:Actor<_>)=
    let mutable registerCount = 0
    let mutable subCount = 0
    let rec loop() = actor{
        let! msg = mailbox.Receive()
        match msg with 
        | Initialize(num) ->      
            for i in 0 .. N-1 do
                workersList.[i] <! "Initialization|"+ ((i+1) |> string)
        | RegistrationBegin(clientlevel) ->   
            registerCount <- registerCount + 1
            if registerCount = N then
                printfn "Registration Completed"
                for i in 0 .. N-1 do
                    workersList.[i] <! "StartSubscription|"   
        | SubscribtionBegin(clientlevel) ->
            subCount <- subCount + 1              
            if subCount = N then
                for i in 0 .. N-1 do
                    workersList.[i] <! "SendRequestToServer|"
         
        return! loop();
    }
    loop()

let dispatcher = spawn system "Boss" Dispatcher

dispatcher <! Initialize(N)

system.WhenTerminated.Wait()