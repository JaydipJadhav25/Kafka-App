const {kafka} = require("./client")

async function init(){
 const admin = kafka.admin();

 console.log("admini connecting.....")

 admin.connect();
 console.log("admini connection success...")

 console.log(" creating topic ......")
 await admin.createTopics({
    topics :[{
        topic : 'rider-update',
        numPartitions : 2,
    },
]
 })

 console.log(" created topic success 'rider-update'")


 

console.log("admin disconnecting......")
await admin.disconnect();
 

}

init();