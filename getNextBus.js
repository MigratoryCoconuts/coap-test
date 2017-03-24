var mongodb = require('mongodb')
	, MongoClient = mongodb.MongoClient
	, co = require('co')
  ,	test = require('assert')
	
function timeStr2Secs(str) {
	if (typeof str == "number" && str < 24*3600) return str;
	
	var flds = str.split(/\W/);	
	return ~~flds[0] * 3600 + ~~flds[1] * 60 + (~~flds[2] || 0);
}

function secsSinceMidnight(d) {
	var e = new Date(d);
	return ~~((d - e.setHours(0,0,0,0))/1000);
}


function aggregateToCollection(col, pipeline, outCol, options) {
	test(pipeline instanceof Array);
	test.equal(typeof outCol, "string");	
	pipeline.push({$out:outCol});
	if(options) return new Promise(function(resolve, reject) {
		col.aggregate(pipeline, options, function(err) {
			if (err) reject(err);
			else resolve();
		});
	});
	else return new Promise(function(resolve, reject) {
		col.aggregate(pipeline, function(err) {
			if (err) reject(err);
			else resolve();
		});
	});
}

function formatDateStr(d) {
	var str = d+"";
	return str.slice(0,4)+'-'+str.slice(4,6)+'-'+str.slice(6,8)
}

function isInDateRange(d, start, end) {
	start = Date.parse(formatDateStr(start));
	if (isNaN(start)) start = 0;

	end = Date.parse(formatDateStr(end)+" 23:59:59.999");
	if (isNaN(end)) end = Infinity;
	
	return (start <= d.valueOf()) && (d.valueOf() <= end);
}

function posMod(a,b) {
	return ((a%b)+b)%b;
}

var requestStop = "1719"
	, requestTime = new Date("2017/03/27 13:33:37");
	

var isValidStop = co.wrap(function*(stop_id){
	var db = yield MongoClient.connect('mongodb://localhost:27017/data');
	return (yield db.collection("stops").find({stop_id:stop_id}).count()) > 0;
});
	
var getNextBus = co.wrap(function*(stop_id,time) {
	var db = yield MongoClient.connect('mongodb://localhost:27017/data');
	
	
	var now = secsSinceMidnight(time)
		, dayOfWeek = (["sunday","monday","tuesday","wednesday","thursday","friday","saturday"])[time.getDay()]
		, UID = "temp"
		, i = 0;
		
	yield aggregateToCollection(db.collection("stoptimes"), [
		{$match:{stop_id:stop_id, arrival_time:{$ne:""}}},	
		{$lookup:{
			from: "trips",
			localField: "trip_id",
			foreignField: "trip_id",
			as: "trip_doc"
		}},
		{$project:{arrival_time:1, route_id:"$trip_doc.route_id", service_id:"$trip_doc.service_id"}},
		{$unwind:"$route_id"},
		{$unwind:"$service_id"},
		{$group:{
			_id: "$service_id",
			arrival_time: {$addToSet:"$arrival_time"},
			route_id: {$addToSet:"$route_id"}
		}},
		{$unwind:"$route_id"},
		{$unwind:"$arrival_time"},
		{$lookup:{
			from: "calendars",
			localField:"_id",
			foreignField:"service_id",
			as:"service_doc"
		}},
		{$project:{start_date:"$service_doc.start_date", end_date:"$service_doc.end_date", arrival_time:1, route_id:1, today:`$service_doc.${dayOfWeek}`,_id:{at:"$arrival_time",sid:"$_id",rid:"$route_id"}}},
		{$unwind:"$start_date"},
		{$unwind:"$end_date"},
		{$unwind:"$today"},
		{$match:{"today":1}}
	], UID);
	
	
	var c = db.collection(UID).find()
		, ops = [];
	
	
	while (yield c.hasNext()){
		var doc = yield c.next();
		ops.push({
			updateOne: {
				filter: {
					_id:doc._id
				},
				update: {
					$set: {
						time_to_wait: posMod(timeStr2Secs(doc.arrival_time)-now,3600*24),		//this will return this morning's buses, needs reworking
						isRunning: isInDateRange(time, doc.start_date, doc.end_date)
					}
				}
			}
		});
	}
	yield db.collection(UID).bulkWrite(ops);
	
	var next5 = db.collection(UID).aggregate([
		{$match:{isRunning:true, time_to_wait:{$lt:3600}}},
		{$sort:{time_to_wait:1}},
		{$project:{_id:0, route_id:1, arrival_time:1, time_to_wait:1}},
		{$limit:5},
	]);
	var out = yield next5.toArray()
	yield db.close();
	return out;
});

//getNextBus(requestStop, new Date())
//	.then(list => {console.dir(list)})
//	.catch(err=>{console.error(err.stack)});

module.exports = {getNextBus: getNextBus, isValidStop: isValidStop};