const {kafka} = require("./client")
const readline  = require("readline")

const rl = readline.createInterface({
    input : process.stdin,
    output : process.stdout,
})


async function init(){
    const producer = kafka.producer();
console.log("connecting producer.....")
 
await producer.connect();

console.log("connected producer successfully.....")


rl.setPrompt(">>");
rl.prompt();

rl.on("line" , async function(line){

    const[riderName , loaction] = line.split(" ")
    await producer.send({
        topic :'rider-update',
        messages : [
            {
                partition : loaction.toLowerCase() === "north" ? 0 : 1,
                key : 'loaction-update' , value : JSON.stringify({name : riderName, loc : loaction}),
            }
        ]
    })

}).on('close' , async()=> {
    await producer.disconnect();

})


// await producer.send({
//     topic :'rider-update',
//     messages : [
//         {
//             partition : 0,
//             key : 'loaction-update' , value : JSON.stringify({name : "jaydip" , loc : "pune"}),
//             // key : 'loaction-update' , value : JSON.stringify({name : "jaydip" , loc : "phaltan"})
//         }
//     ]
// })

// console.log("producer disconnected.......");



}

init();