const {kafka} = require("./client")

async function init(){
    const producer = kafka.producer();
console.log("connecting producer.....")
 
await producer.connect();

console.log("connected producer successfully.....")

await producer.send({
    topic :'rider-update',
    messages : [
        {
            partition : 0,
            key : 'loaction-update' , value : JSON.stringify({name : "jaydip" , loc : "pune"}),
            // key : 'loaction-update' , value : JSON.stringify({name : "jaydip" , loc : "phaltan"})
        }
    ]
})

console.log("producer disconnected.......");

await producer.disconnect();


}

init();