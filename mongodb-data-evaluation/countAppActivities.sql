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
///990101-990131  535,606    ?      (app)-peyman query result => diff=2,450
///990101-990131  533,665    49s    (app)-alireza query result => diff=509



db.activities.countDocuments({creationDate:{$gte:1584649800000,$lte:1587324599999}
    ,status: 0,type: {$in: [50,60,100]}})
;//990101-990131  4,900 7s  (not-listed-services)


activities.aggregate([
    {
        '$match': {
            'creationDate': {'$gte': 1532503800000, '$lte': 1604478600000},
            'status': 0,
            'type': {'$in': [12, 15, 16, 17, 30, 31, 32, 40, 70, 80, 92, 110, 111, 112, 113, 140, 170]}
            (12 ,15, 16, 17, 30, 31, 32, 40, 50, 60, 70, 80, 92, 100, 110, 111, 112, 113)
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
        '$project': {
            '_id': '$_id',
            'owner': '$owner',
            'app_activities': {'$cond': [{}, 1, 0]},
            'topUp_activities': {'$cond': [{'$in': ['$type', [30, 31]]}, 1, 0]},
            'bill_activities': {'$cond': [{'$eq': ['$type', 40]}, 1, 0]},
        }
    },
    {
        '$group': {
            '_id': null,
            'total_app_activities': {'$sum': '$app_activities'},
            'total_topUp_activities': {'$sum': '$topUp_activities'},
            'total_bill_activities': {'$sum': '$bill_activities'},
        }
    }
], {allowDiskUse: true});


