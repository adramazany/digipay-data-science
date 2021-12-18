report_db.activities.aggregate([
    {$match: {
            "creationDate": {$gte: step_start, $lt: step_end},
            "gateway": {$in: [0,1]},
            "type": {$in: psp_by_gateway},
            "ownerSide": {$in: [0,3]}
        }},
    {$group: {
            "_id" : "$owner.username",
            "status": {$addToSet: "$status"}
        }}
], {allowDiskUse:true});
