//db.createCollection('bi_temp');--unauthorized
db.activities.find({}).sort({creationDate:1} ).limit(10);
db.activities.count({});

db.activities.getIndexes();
db.activities.find({},{creationDate:1,$rename() "$creationDate"}).sort({creationDate:1} ).limit(1);
db.activities.find({},{creationDate:1, amount:1, "field1":{$exits:true,$amount }}).sort({creationDate:1} ).limit(1);

/////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////// test mongo - ipg users  ////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////
db.activities.aggregate([
    {$match: {
            creationDate: {$gte:1584649800000,$lte:1587324599999}
            ,gateway: {$in: [0,1]}
            ,type: {$in: [0, 1, 16, 30, 31, 40, 70, 80, 92, 100, 110, 111, 112, 113, 130, 140, 150, 160, 170]}
            ,ownerSide: {$in: [0,3]}
            ,status: 0
        }}
    ,{$group: {
            _id : "$trackingCode"
            ,username : {$first:"$owner.username"}
            ,gateways:  {$first:"$gateway"}
            ,status:   {$first:"$status"}
            ,ownerSide:   {$addToSet:"$ownerSide"}
            ,type:   {$first:"$type"}
        }}
    ,{$group: {
            _id : {username:"$username",type:"$type"}
            ,tnx : {$sum: 1}
            ,succeed   : {$sum: {$cond : [ {$eq:["$status",0]}  ,1,0] }}
            ,failed    : {$sum: {$cond : [ {$ne:["$status",0]}  ,1,0] }}
            ,gateway_0 : {$sum: {$cond:[ {$eq:[0,"$gateways"]}   ,1,0 ]}}
            ,gateway_1 : {$sum: {$cond:[ {$eq:[1,"$gateways"]}   ,1,0 ]}}
            ,ownerSide_0 : {$sum: {$cond:[ {$in:[0,"$ownerSide"]}   ,1,0 ]}}
            ,ownerSide_3 : {$sum: {$cond:[ {$in:[3,"$ownerSide"]}   ,1,0 ]}}
            ,type        : {$addToSet:"$type"}
        }}
    ,{ $group : {_id :"$_id.type"
            ,users:{$sum:1}
            ,tnxs : {$sum : '$tnx' }
            ,succeed : {$sum : '$succeed' }
            ,failed : {$sum : '$failed' }
            ,gateway_0 : {$sum : '$gateway_0' }
            ,gateway_1 : {$sum : '$gateway_1' }
            ,ownerSide_0 : {$sum : '$ownerSide_0' }
            ,ownerSide_3 : {$sum : '$ownerSide_3' }
        }}
], {allowDiskUse:true})

//990101-990131 12m50s  tnx:1,929,161   users:1,106,089

/////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////// distinct users all activities   ////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////
db.activities.aggregate([
    {$match: {
             creationDate: {$gte:1584649800000,$lte:1584736199999}
            ,gateway: {$in: [0,1]}
            ,type: {$in: [0, 1, 16, 30, 31, 40, 70, 80, 92, 100, 110, 111, 112, 113, 130, 140, 150, 160, 170]}
            ,ownerSide: {$in: [0,3]}
            ,status: 0
        }}
    ,{$group: {
            _id : "$trackingCode"
            ,username : {$first:"$owner.username"}
            ,gateways:  {$first:"$gateway"}
            ,status:   {$first:"$status"}
            ,ownerSide:   {$addToSet:"$ownerSide"}
            ,type:   {$first:"$type"}
        }}
    ,{$group: {
            _id : {username:"$username",type:"$type"}
            ,tnx : {$sum: 1}
            ,succeed   : {$sum: {$cond : [ {$eq:["$status",0]}  ,1,0] }}
            ,failed    : {$sum: {$cond : [ {$ne:["$status",0]}  ,1,0] }}
            ,gateway_0 : {$sum: {$cond:[ {$eq:[0,"$gateways"]}   ,1,0 ]}}
            ,gateway_1 : {$sum: {$cond:[ {$eq:[1,"$gateways"]}   ,1,0 ]}}
            ,ownerSide_0 : {$sum: {$cond:[ {$in:[0,"$ownerSide"]}   ,1,0 ]}}
            ,ownerSide_3 : {$sum: {$cond:[ {$in:[3,"$ownerSide"]}   ,1,0 ]}}
            ,type        : {$addToSet:"$type"}
        }}
    ,{ $group : {_id :"$_id.type"
            ,users:{$sum:1}
            ,tnxs : {$sum : '$tnx' }
            ,succeed : {$sum : '$succeed' }
            ,failed : {$sum : '$failed' }
            ,gateway_0 : {$sum : '$gateway_0' }
            ,gateway_1 : {$sum : '$gateway_1' }
            ,ownerSide_0 : {$sum : '$ownerSide_0' }
            ,ownerSide_3 : {$sum : '$ownerSide_3' }
    }}
], {allowDiskUse:true})
;
//990101    11s     36,329




/////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////// test   ////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////



db.activities.aggregate([
    {$sort:{creationDate:1}}
    ,{$limit: 10}
    ,{$project: {creationDate:1
            , cd:{$cond :[ {$gt:["$creationDate",1540122956108]} ,1,0] }
            , cd2:{$toDate : "$creationDate"}
            , cd3:{$dateToString : {format:"%Y-%m-%d", date:{$toDate:"$creationDate"}}}
            }}
    ,{$group:{_id:"$cd3",count:{$sum:1}
                ,count_a:{$sum:"$cd"}
                ,count_b:{$sum : {$cond: [{$eq:["$cd",0]} ,1,0 ]} }

        }}
], {allowDiskUse:true});

// start :  1539887493518
//end    :  1606124443694

//ipg-total-customers
db.activities.aggregate([
    {$match: {
            creationDate: {$gte:1584649800000,$lte:1584736199999},
            gateway: {$in: [0,1]},
            type: {$in: [0, 1, 16, 30, 31, 40, 70, 80, 92, 100, 110, 111, 112, 113, 130, 140, 150, 160, 170]},
            ownerSide: {$in: [0,3]},
            status: 0
        }}
    ,{$group: {
            _id : "$owner.username",
            count : {"$first": 1}
        }}
    ,{ $group : {_id : null , count2 : {$sum : '$count' }}}
], {allowDiskUse:true})
;
//990101    11s     36,329

//ipg-total-users
db.activities.aggregate([
    {$match: {
            creationDate: {$gte:1584649800000,$lte:1584818999999},
            gateway: {$in: [0,1]},
            type: {$in: [0, 1, 16, 30, 31, 40, 70, 80, 92, 100, 110, 111, 112, 113, 130, 140, 150, 160, 170]},
            ownerSide: {$in: [0,3]}
        }}
    ,{$group: {
            _id : "$owner.username"
            ,count : {"$first": 1}
            ,day : {$addToSet: { $dateToString: { format: "%Y-%m-%d", date: "$creationDate" } } }
        }}
    ,{ $group : {_id : null , count2 : {$sum : '$count' }}}
], {allowDiskUse:true})
;
//990101            26s     38,540
//990101-990102     27s     69,266


/////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////// count IPG Users   ////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////

db.activities.aggregate([
    {$match: {creationDate:{$gte:1584649800000,$lte:1584736199999}
            ,gateway: {$in: [0,1]} ,ownerSide: {$in: [0,3]}
            ,type: {$in: [0, 1, 16, 30, 31, 40, 70, 80, 92, 100, 110, 111, 112, 113, 130, 140, 150, 160, 170]}
        }},
    {$group: {
            "_id" : "$owner.username",
            "status": {$addToSet: "$status"}
        }}
], {allowDiskUse:true})
;//990101-990631  0 403ms  (not-listed-services)

/////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////// count App Activities   ////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////

db.activities.find({}).limit(10);
db.activities.count({creationDate:{$gte:1584649800000,$lte:1584736199999} });//254,612 403ms
db.activities.find({creationDate:{$gte:1584649800000,$lte:1584736199999} }
,{_id:0,trackingCode:1,creationDate:1,type:1,gateway:1,ownerSide:1,actionCode:1})
    .limit(10)
;//254,612 403ms

db.activities.aggregate([
    { $match : {creationDate:{$gte:1584649800000,$lte:1584736199999}
            ,status: 0,type: {$in: [12, 15, 16, 17, 30, 31, 32, 40, 70, 80, 92, 110, 111, 112, 113, 140, 170]}
        } }
    ,{ $group : {_id:'$trackingCode',count : {$first : 1}
        } }
    ,{ $group : {_id : null , count2 : {$sum : '$count' }}}
], {allowDiskUse: true})
;//990101   125,289  1s (all)
;//990101   16,695   8s (app)
db.activities.aggregate([
    { $match : {creationDate:{$gte:1587238200000,$lte:1587324599999}
            ,status: 0,type: {$in: [12, 15, 16, 17, 30, 31, 32, 40, 70, 80, 92, 110, 111, 112, 113, 140, 170]}
        } }
    ,{ $group : {_id:'$trackingCode',count : {$first : 1}
        } }
    ,{ $group : {_id : null , count2 : {$sum : '$count' }}}
], {allowDiskUse: true})
;//990131   20,996  13s


db.activities.aggregate([
    { $match : {creationDate:{$gte:1584649800000,$lte:1587324599999}
                ,status: 0
                ,type: {$in: [12, 15, 16, 17, 30, 31, 32, 40, 70, 80, 92, 110, 111, 112, 113, 140, 170]}
    } }
    ,{ $group : {_id:'$trackingCode'
            ,count : {$first : 1}
        } }
    ,{ $group : {_id : null , count2 : {$sum : '$count' }}}
], {allowDiskUse: true})
;
///990101-990131  5,481,143  15m18s (all)
///990101-990131  533,156    15m35s (app)
///990101-990131  535,606    15m35s (app)-peyman query result => diff=2,450
///990101-990131  533,665    49s    (app)-alireza query result => diff=509



db.activities.countDocuments({creationDate:{$gte:1584649800000,$lte:1587324599999}
    ,status: 0,type: {$in: [50,60,100]}})
;//990101-990131  4,900 7s  (not-listed-services)
db.activities.countDocuments({creationDate:{$gte:1584649800000,$lte:1600720199999}
    ,status: 0,type: {$in: [50,60]}})
;//990101-990631  0 403ms  (not-listed-services)
db.activities.countDocuments({creationDate:{$gte:1584649800000,$lte:1600720199999}
    ,status: 0,type: {$in: [100]}})
;//990101-990631  103,496 10m15s  (not-listed-services)


db.activities.aggregate([
    {
        '$match': {
            'creationDate': {'$gte': 1600720200000, '$lte': 1603312199999},
            'status': 0,
            'type': {'$in': [112]}
        }
    },
    {
        '$group': {
            '_id': '$trackingCode',
            'owner': {'$first': '$source.owner.username'},
            'type': {'$first': '$type'}
        }
    },
     {
        '$group': {
            '_id': null,
            'total_app_activities': {'$sum': 1},
         }
    }
], {allowDiskUse: true});



######################################
############     payment ipg   #############
############     active user,customer,active customer   #############
############     mehr 99       #############
######################################

db.activities.aggregate([
    {$match: {
            creationDate: {$lt: 1603312199999}
            ,gateway: {$in: [0,1]}
            ,type: {$in: [0, 1, 16, 30, 31, 40, 70, 80, 92, 100, 110, 111, 112, 113, 130, 140, 150, 160, 170]}
            ,ownerSide: {$in: [0,3]}
        }}
    ,{$group: {
            _id : "$owner.username"
            ,"status": {$addToSet: "$status"}
            ,"lastTnxDate": {$last: "$creationDate"}
        }}
    ,{ $group : {
            _id : null
            ,ipg_active_user:{$sum:1}
            ,ipg_customer:{$sum: {$cond : [ {$in:[0,"$status"]}  ,1,0] }}
            ,ipg_active_customer:{$sum: {$cond : [ {$gte:["$lastTnxDate",1587324600000]}  ,1,0] }}
        }}
], {allowDiskUse:true})
;
