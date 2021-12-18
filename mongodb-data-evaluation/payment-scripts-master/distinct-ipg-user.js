load("jalali-moment.js");

const jalaliStart_inclusive = moment('1397/07/26', 'jYYYY/jMM/jDD');
const jalaliEnd_exclusive = moment('1399/08/01', 'jYYYY/jMM/jDD');
const exlusiveMilestones = ["1399/02/01", "1399/03/01", "1399/04/01","1399/05/01","1399/06/01","1399/07/01", "1399/08/01"];

const epoch_start = jalaliStart_inclusive.toDate().getTime();
const epoch_end = jalaliEnd_exclusive.toDate().getTime();
const step_by_day = 1;

const psp_by_gateway = [0, 1, 16, 30, 31, 40, 70, 80, 92, 100, 110, 111, 112, 113, 130, 140, 150, 160, 170];

const mongo = new Mongo("172.16.27.11:27017");
mongo.setSecondaryOk();
const admin_db = mongo.getDB("admin");
admin_db.auth("***", "***");
const report_db = admin_db.getSiblingDB("report_mng_db");

function calc_distinct(step_start, step_end) {
    return report_db.activities.aggregate([
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
    ], {allowDiskUse:true}).toArray();
}

function toPersianDate(date) {
    return moment(new Date(date), 'YYYY/MM/DD').locale('fa').format('YYYY/MM/DD');
}

function addStep(timestamp) {
    let persianDate = moment(new Date(timestamp), 'YYYY/MM/DD');
    persianDate.add(step_by_day, 'day');
    return persianDate.toDate().getTime();
}

var snapshots = {};
function takeSnapShot(step_end, users, customers) {
    snapshots[step_end] = {
        "users" : users.size,
        "customers" : customers.size
    };
}

print("start,end,total users,new users,total customers,new customers");

var totalUsers = new Set();
var totalCustomers = new Set();
var step_start = epoch_start;

while (step_start < epoch_end) {
    var step_end = Math.min(addStep(step_start), epoch_end);
    try {
        var users = calc_distinct(step_start, step_end);
        let usersSize = totalUsers.size;
        let customersSize = totalCustomers.size;
        let successCount = 0;
        for (var i = 0; i < users.length; i++) {
            let user = users[i];
            totalUsers.add(user._id);
            if (user.status.includes(0)) {
                successCount++;
                totalCustomers.add(user._id);
            }
        }
        let newUsers = totalUsers.size - usersSize;
        let newCustomers = totalCustomers.size - customersSize;
        print(toPersianDate(step_start) + "," + toPersianDate(step_end) + "," + users.length
            + "," + newUsers + "," + successCount + "," + newCustomers);
        if (exlusiveMilestones.includes(toPersianDate(step_end))) {
            takeSnapShot(toPersianDate(step_end), totalUsers, totalCustomers);
        }
        step_start = step_end;
    } catch (err) {
        print("error on step " + step_start + " - " + step_end + ": " + err);
    }
}
print(toPersianDate(epoch_start) + "," + toPersianDate(epoch_end) + ", ," + totalUsers.size + ", ," + totalCustomers.size);

print(",,,,,,,,,");
print(",,,,,,,,,");

for (let key in snapshots) {
    print(key + "," + snapshots[key].users + "," + snapshots[key].customers);
}