load("jalali-moment.js");

//TODO: get durations from input
const jalaliStart_inclusive = moment('1399/01/01', 'jYYYY/jMM/jDD');
const jalaliEnd_exclusive = moment('1399/02/01', 'jYYYY/jMM/jDD');

const epoch_start = jalaliStart_inclusive.toDate().getTime();;
const epoch_end = jalaliEnd_exclusive.toDate().getTime();
const step_by_day = 1;
const print_step_stats = true;

//tx types that are always counted in wallet stats
const wallet_by_type = [15, 16, 17];

//NOTE: logically wallet auto-refunds should be counted based on their destination type not their gateway
// but some microservices (e.g. topup) do not send a destination type which defaults to CARD.

//tx types that are counted in wallet stats if their gateways are wallet
const wallet_by_gateway = [0, 1, 3, 4, 30, 31, 32, 40, 70, 80, 92, 100, 110, 111, 112, 113, 130, 140, 150, 160, 170];

//tx types that are counted in wallet stats if their destinations are wallet
const wallet_by_destination = [10, 91, 93, 120];

//tx types that are are always counted in psp stats
const psp_by_type = [16];

//tx types that are counted in psp stats if their gateways are ipg or dpg
const psp_by_gateway = [0, 1, 30, 31, 40, 70, 80, 92, 100, 110, 111, 112, 113, 130, 140, 150, 160, 170];

//tx types that are considered as purchase
const purchase_types = [0, 1];

//tx types that are considered as spend
const spend_types = [0, 1, 30, 31, 32, 40, 70, 80, 92, 100, 110, 111, 112, 113, 130, 140, 150, 160, 170];

//digikala users (digikala, digistyle)
const dk_users = ['dk-ipg', 'dgstyle-ipg'];

const mongo = new Mongo("172.16.27.11:27017");
mongo.setSecondaryOk();
const admin_db = mongo.getDB("admin");
admin_db.auth("***", "***");
const report_db = admin_db.getSiblingDB("report_mng_db");

function calc_step_stats(step_start, step_end) {
    const cursor = report_db.activities.aggregate([
        {$match:{
            'creationDate':{$gte: step_start, $lt: step_end},
            'status': 0
        }},
        {$group:{
            '_id': '$trackingCode',
            'amount': {$first: '$amount'},
            'type': {$first: '$type'},
            'gateway': {$first: '$gateway'},
            'destinationType': {$first: '$destination.endpointType'},
            'destinationCell': {$first: '$destination.owner.cellNumber'},
            'sourceCell': {$first: '$source.owner.cellNumber'}
        }},
        {$project:{
            '_id': 0,
            'amount': 1,
            'wallet_cashback': {$cond: [{$and:[{$eq:['$gateway', 4]}, {$eq:['$type', 4]}]}, 1, 0]},
            'wallet_purchase': {$cond: [{$and:[{$eq:['$gateway', 4]}, {$in:['$type', purchase_types]}]}, 1, 0]},
            'wallet_spend': {$cond: [{$and:[{$eq:['$gateway', 4]}, {$in:['$type', spend_types]}]}, 1, 0]},
            'wallet_transfer': {$cond: [{$eq:['$type', 15]}, 1, 0]},
            'wallet_cashin': {$cond: [{$eq:['$type', 16]}, 1, 0]},
            'wallet_cashout': {$cond: [{$eq:['$type', 17]}, 1, 0]},
            'wallet_refund':  {$cond: [{$or:[
                {$and:[{$eq:['$gateway', 4]}, {$eq:['$type', 3]}]},
                {$and:[{$eq:['$destinationType', 4]}, {$in:['$type', [91, 93]]}]}
            ]}, 1, 0]},
            'wallet_total': {$cond: [{$or:[
                {$and:[{$eq:['$gateway', 4]}, {$in:['$type', wallet_by_gateway]}]},
                {$and:[{$eq:['$destinationType', 4]}, {$in:['$type', wallet_by_destination]}]},
                {$in:['$type', wallet_by_type]}
            ]}, 1, 0]},

            'psp_purchase_dk': {$cond: [{$and:[
                {$in:['$gateway', [0, 1]]},
                {$in:['$type', purchase_types]},
                {$in:['$destinationCell', dk_users]}
            ]}, 1, 0]},
            'psp_purchase_fidibo': {$cond: [{$and:[
                {$in:['$gateway', [0, 1]]},
                {$in:['$type', purchase_types]},
                {$eq:['$destinationCell', 'fidibo']}
            ]}, 1, 0]},
            'psp_purchase_all': {$cond: [{$and:[{$in:['$gateway', [0, 1]]}, {$in:['$type', purchase_types]}]}, 1, 0]},
            'psp_total': {$cond: [{$or:[
                {$and:[{$in:['$gateway', [0, 1]]}, {$in:['$type', psp_by_gateway]}]},
                {$in:['$type', psp_by_type]}
            ]}, 1, 0]},

            'refund_auto_dk': {$cond: [{$and:[{$eq:['$type', 3]}, {$in:['$sourceCell', dk_users]}]}, 1, 0]},
            'refund_manual_dk': {$cond: [{$and:[{$eq:['$type', 91]}, {$in:['$sourceCell', dk_users]}]}, 1, 0]},
            'refund_custom_dk': {$cond: [{$and:[{$eq:['$type', 93]}, {$in:['$sourceCell', dk_users]}]}, 1, 0]},
            'refund_auto_all': {$cond: [{$eq:['$type', 3]}, 1, 0]},
            'refund_manual_all': {$cond: [{$eq:['$type', 91]}, 1, 0]},
            'refund_custom_all': {$cond: [{$eq:['$type', 93]}, 1, 0]}
        }},
        {$group:{
            '_id': null,

            'wallet_cashback_count': {$sum: '$wallet_cashback'},
            'wallet_cashback_amount': {$sum: {$multiply:['$amount', '$wallet_cashback']}},
            'wallet_purchase_count': {$sum: '$wallet_purchase'},
            'wallet_purchase_amount': {$sum: {$multiply:['$amount', '$wallet_purchase']}},
            'wallet_spend_count': {$sum: '$wallet_spend'},
            'wallet_spend_amount': {$sum: {$multiply:['$amount', '$wallet_spend']}},
            'wallet_transfer_count': {$sum: '$wallet_transfer'},
            'wallet_transfer_amount': {$sum: {$multiply:['$amount', '$wallet_transfer']}},
            'wallet_cashin_count': {$sum: '$wallet_cashin'},
            'wallet_cashin_amount': {$sum: {$multiply:['$amount', '$wallet_cashin']}},
            'wallet_cashout_count': {$sum: '$wallet_cashout'},
            'wallet_cashout_amount': {$sum: {$multiply:['$amount', '$wallet_cashout']}},
            'wallet_refund_count': {$sum: '$wallet_refund'},
            'wallet_refund_amount': {$sum: {$multiply:['$amount', '$wallet_refund']}},
            'wallet_total_count': {$sum: '$wallet_total'},
            'wallet_total_amount': {$sum: {$multiply:['$amount', '$wallet_total']}},

            'psp_purchase_dk_count': {$sum: '$psp_purchase_dk'},
            'psp_purchase_dk_amount': {$sum: {$multiply:['$amount', '$psp_purchase_dk']}},
            'psp_purchase_fidibo_count': {$sum: '$psp_purchase_fidibo'},
            'psp_purchase_fidibo_amount': {$sum: {$multiply:['$amount', '$psp_purchase_fidibo']}},
            'psp_purchase_all_count': {$sum: '$psp_purchase_all'},
            'psp_purchase_all_amount': {$sum: {$multiply:['$amount', '$psp_purchase_all']}},
            'psp_total_count': {$sum: '$psp_total'},
            'psp_total_amount': {$sum: {$multiply:['$amount', '$psp_total']}},

            'refund_auto_dk_count': {$sum: '$refund_auto_dk'},
            'refund_auto_dk_amount': {$sum: {$multiply:['$amount', '$refund_auto_dk']}},
            'refund_manual_dk_count': {$sum: '$refund_manual_dk'},
            'refund_manual_dk_amount': {$sum: {$multiply:['$amount', '$refund_manual_dk']}},
            'refund_custom_dk_count': {$sum: '$refund_custom_dk'},
            'refund_custom_dk_amount': {$sum: {$multiply:['$amount', '$refund_custom_dk']}},
            'refund_auto_all_count': {$sum: '$refund_auto_all'},
            'refund_auto_all_amount': {$sum: {$multiply:['$amount', '$refund_auto_all']}},
            'refund_manual_all_count': {$sum: '$refund_manual_all'},
            'refund_manual_all_amount': {$sum: {$multiply:['$amount', '$refund_manual_all']}},
            'refund_custom_all_count': {$sum: '$refund_custom_all'},
            'refund_custom_all_amount': {$sum: {$multiply:['$amount', '$refund_custom_all']}}
        }}
    ], {allowDiskUse: true});
    return cursor.hasNext() ? cursor.next() : {};
}

function toPersianDate(date){
	return moment(new Date(date), 'YYYY/MM/DD').locale('fa').format('YYYY/MM/DD');
}

function addStep(timestamp) {
	let persianDate = moment(new Date(timestamp), 'YYYY/MM/DD');
	persianDate.add(step_by_day, 'day');
	return persianDate.toDate().getTime();
}

function add_step_stats(stats, step_stats) {
    for (let stat in stats) {
        stats[stat] += (step_stats[stat] || 0);
    }
}

function printStats(stats, start, end) {
    let result = toPersianDate(start) + "," + toPersianDate(end);
    for (let i = 0; i < keys.length; i++) {
        result += "," + (stats[keys[i]] || 0);
    }
    print(result);
}

var step_start = epoch_start;
var total_stats = {
    wallet_cashback_count: 0,
    wallet_cashback_amount: 0,
    wallet_purchase_count: 0,
    wallet_purchase_amount: 0,
    wallet_spend_count: 0,
    wallet_spend_amount: 0,
    wallet_transfer_count: 0,
    wallet_transfer_amount: 0,
    wallet_cashin_count: 0,
    wallet_cashin_amount: 0,
    wallet_cashout_count: 0,
    wallet_cashout_amount: 0,
    wallet_refund_count: 0,
    wallet_refund_amount: 0,
    wallet_total_count: 0,
    wallet_total_amount: 0,

    psp_purchase_dk_count: 0,
    psp_purchase_dk_amount: 0,
    psp_purchase_fidibo_count: 0,
    psp_purchase_fidibo_amount: 0,
    psp_purchase_all_count: 0,
    psp_purchase_all_amount: 0,
    psp_total_count: 0,
    psp_total_amount: 0,

    refund_auto_dk_count: 0,
    refund_auto_dk_amount: 0,
    refund_manual_dk_count: 0,
    refund_manual_dk_amount: 0,
    refund_custom_dk_count: 0,
    refund_custom_dk_amount: 0,
    refund_auto_all_count: 0,
    refund_auto_all_amount: 0,
    refund_manual_all_count: 0,
    refund_manual_all_amount: 0,
    refund_custom_all_count: 0,
    refund_custom_all_amount: 0
};

let keys = Object.keys(total_stats);
print("start,end," + keys.toString());

while (step_start < epoch_end) {
    let step_end = Math.min(addStep(step_start), epoch_end);
    try {
        let step_stats = calc_step_stats(step_start, step_end);
        add_step_stats(total_stats, step_stats);

        if (print_step_stats) {
            printStats(step_stats, step_start, step_end);
        }

        step_start = step_end;
    } catch (err) {
        print("error on step " + step_start + " - " + step_end + ": " + err);
    }
}

printStats(total_stats, epoch_start, epoch_end);