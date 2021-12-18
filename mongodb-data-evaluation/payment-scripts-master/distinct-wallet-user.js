load("jalali-moment.js");

const jalaliStart_inclusive = moment('1397/07/26', 'jYYYY/jMM/jDD');
const jalaliEnd_exclusive = moment('1399/08/01', 'jYYYY/jMM/jDD');
const exlusiveMilestones = ["1399/02/01", "1399/03/01", "1399/04/01","1399/05/01","1399/06/01","1399/07/01", "1399/08/01"];

const epoch_start = jalaliStart_inclusive.toDate().getTime();
const epoch_end = jalaliEnd_exclusive.toDate().getTime();
const step_by_day = 1;

const spend_types = [0, 1, 30, 31, 32, 40, 70, 80, 92, 100, 110, 111, 112, 113, 130, 140, 150, 160, 170];
const wallet_by_type = [15, 16, 17];

const mongo = new Mongo("172.16.27.11:27017");
mongo.setSecondaryOk();
const admin_db = mongo.getDB("admin");
admin_db.auth("***", "***");
const report_db = admin_db.getSiblingDB("report_mng_db");

// TODO : the logic is a bit suspecious.
function getUserId(activity) {
    let users = new Array();
    if (spend_types.includes(activity.type) || activity.type === 17) {
        users.push(activity.source);
    } else if (activity.type === 15) {
        users.push(activity.source,activity.destination);
    } else {
        users.push(activity.destination);
    }
    return users;
}

function getActivities(step_start, step_end) {
    return report_db.activities.aggregate([
        {$match: {$and: [
            {"creationDate": {$gte: step_start, $lt: step_end}, "status": 0},
            {$or: [
                {"gateway": 4, "type": {$in: spend_types}},
                {"type": {$in: wallet_by_type}}
            ]}
        ]}},
        {$group: {
            "_id": "$trackingCode",
            "type": {$first : "$type"},
            "source": {$first: "$source.owner.username"},
            "destination": {$first: "$destination.owner.username"}
        }},
    ], {allowDiskUse:true}).toArray();
}

function toPersianDate(date){
	return moment(new Date(date), 'YYYY/MM/DD').locale('fa').format('YYYY/MM/DD');
}

function addStep(timestamp) {
	let persianDate = moment(new Date(timestamp), 'YYYY/MM/DD');
	persianDate.add(step_by_day, 'day');
	return persianDate.toDate().getTime();
}

var snapshots = {};
function takeSnapShot(step_end, customers){
	snapshots[step_end] = {
		"customers" : customers.size
	};
}

print("start,end,customers,new cusomers");

var totalCustomers = new Set();
var step_start = epoch_start;

while (step_start < epoch_end) {
    var step_end = Math.min(addStep(step_start), epoch_end);
    try {
        let activities = getActivities(step_start, step_end);
        let customersSize = totalCustomers.size;
        let users = new Set();
        for (var i = 0; i < activities.length; i++) {
            let activity = activities[i];
            let userIdList = getUserId(activity);
            userIdList.forEach(item => users.add(item));
        }
        users.forEach(item => totalCustomers.add(item));
        let newCustomers = totalCustomers.size - customersSize;
        print(toPersianDate(step_start) + "," + toPersianDate(step_end) + "," + users.size + "," + newCustomers);
        if (exlusiveMilestones.includes(toPersianDate(step_end))) {
            takeSnapShot(toPersianDate(step_end), totalCustomers);
        }
        step_start = step_end;
    } catch (err) {
        print("error on step " + step_start + " - " + step_end + ": " + err);
    }
}
print(toPersianDate(step_start) + "," + toPersianDate(step_end) + "," + totalCustomers.size);

print(",,,,,,,,,");
print(",,,,,,,,,");

for (let key in snapshots) {
	print(key + "," + snapshots[key].customers);
}
