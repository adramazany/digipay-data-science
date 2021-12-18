const MongoClient = require('mongodb').MongoClient;
const moment = require('jalali-moment');
moment.locale('fa')

const step = 24 * 60 * 60 * 1000;

const mongoCred = {
	uri: "172.16.27.11:27017",
	username: "ops",
	password: "ops%402020",
	dbName: 'admin',
};

const mongoUri = `mongodb://${mongoCred.username}:${mongoCred.password}@${mongoCred.uri}/${mongoCred.dbName}`;

const client = new MongoClient(mongoUri, {
	useNewUrlParser: true,
	autoReconnect: true,
	keepAlive: 1,
	connectTimeoutMS: 12000000,
	socketTimeoutMS: 12000000,
	readPreference: 'secondaryPreferred'
});

const appActivityTypes = [12, 15, 16, 17, 30, 31, 32, 40, 70, 80, 92, 100, 110, 111, 112, 113, 140, 170];


async function countAppActivities(collection, startDate, endDate) {
	let activitiesCount = {
		appActivities: 0,
		topUpActivities: 0,
		billActivities: 0,
	};
	await divideQueryTimeSeries(
		startDate, endDate,
		countAppActivitiesQuery.bind(this, collection),
		aggregateAppActivitiesResults.bind(this, activitiesCount)
	)
	console.log(`app activities: ${activitiesCount.appActivities}`);
	console.log(`topUp activities: ${activitiesCount.topUpActivities}`);
	console.log(`bill activities: ${activitiesCount.billActivities}`);
}

async function countAppActivitiesQuery(collection, startDate, endDate) {
	return collection.aggregate([
		{
			'$match': {
				'creationDate': {'$gte': startDate, '$lt': endDate},
				'status': 0,
				'type': {'$in': appActivityTypes}
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
	], {allowDiskUse: true}).toArray();
}

function aggregateAppActivitiesResults(activitiesCount, tempResult) {
	if (tempResult == null || tempResult.length === 0) {
		return;
	}

	if (tempResult[0]['total_app_activities']) {
		activitiesCount.appActivities += tempResult[0]['total_app_activities'];
	}
	if (tempResult[0]['total_topUp_activities']) {
		activitiesCount.topUpActivities += tempResult[0]['total_topUp_activities'];
	}
	if (tempResult[0]['total_bill_activities']) {
		activitiesCount.billActivities += tempResult[0]['total_bill_activities'];
	}
}

async function countActiveCustomers(collection, startDate, endDate) {
	let result = new Set();
	await divideQueryTimeSeries(
		startDate, endDate,
		countActiveCustomersQuery.bind(this, collection),
		aggregateResults.bind(this, result)
	)
	console.log(result.size);
}

async function countActiveCustomersQuery(collection, startDate, endDate) {
	return collection.aggregate([
		{
			'$match': {
				'creationDate': {'$gte': startDate, '$lt': endDate},
				'status': 0,
				'type': {'$in': appActivityTypes}
			}
		},
		{'$group': {'_id': '$source.owner.username'}}
	], {allowDiskUse: true}).toArray();
}

function aggregateResults(endResult, tempResult) {
	for (const item of tempResult) {
		endResult.add(item._id)
	}
	return endResult;
}

async function divideQueryTimeSeries(startDate, endDate, query, aggregator) {
	while (startDate < endDate) {
		let stepEnd = Math.min(startDate + step, endDate);
		try {
			const tempResult = await query(startDate, stepEnd)
			await aggregator(tempResult);
			startDate = stepEnd;
		} catch (err) {
			console.log('well something bad happened', err);
		}
	}
}

client.connect(async err => {

	if (err) {
		console.log(err);
		throw new Error("err connecting to mongodb");
	}

	console.log("Connected successfully to server");

	const db = client.db("report_mng_db");
	const collection = db.collection('activities');
	try {
		for (let i = 1; i < 8; i++) {
			let startDate = moment(`1399/0${i}/01`, 'jYYYY/jMM/jDD').unix() * 1000;
			let endDate = moment(`1399/0${i + 1}/01`, 'jYYYY/jMM/jDD').unix() * 1000;
			console.log(startDate, endDate);
			await countAppActivities(collection, startDate, endDate);

			startDate = moment(`1399/0${i}/01`, 'jYYYY/jMM/jDD').subtract(5, 'months').unix() * 1000;
			endDate = moment(`1399/0${i + 1}/01`, 'jYYYY/jMM/jDD').unix() * 1000;
			console.log(startDate, endDate);
			await countActiveCustomers(collection, startDate, endDate);

			startDate = 1532503800000;
			endDate = moment(`1399/0${i + 1}/01`, 'jYYYY/jMM/jDD').unix() * 1000;
			await countActiveCustomers(collection, startDate, endDate);
		}
	} catch (e) {
		console.log("script ended, we have an err");
		console.log(e)
		console.log("see ya soon!")
	}

	await client.close();

	console.log("bye!");

});