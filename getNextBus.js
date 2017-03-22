var mongodb = require('mongodb')
	, MongoClient = mongodb.MongoClient
	, co = require('co')
  ,	test = require('assert');
	
function timeStr2Secs(str) {
	if (typeof str == "number" && str < 24*3600) return str;
	var flds = str.split(/\W/);
		
	return ~~flds[0] * 3600 + ~~flds[1] * 60 + (~~flds[2] || 0);
	
}

function secsSinceMidnight(d) {
	var e = new Date(d);
	return ~~((d - e.setHours(0,0,0,0))/1000);
}

//console.log(mongodb.Collection.prototype);

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

function posMod(a,b) {
return ((a%b)+b)%b;
}

var requestStop = "1719"
	, requestTime = new Date("2017/03/27 13:33:37");
	
console.log(requestTime.toLocaleString());
	
var getNextBus = co.wrap(function*(stop_id,time) {
	var db = yield MongoClient.connect('mongodb://localhost:27017/data');
	
	console.log("connected");
	
	var now = secsSinceMidnight(time)
		, doyOfWeek = 
		, UID = "test" // Date.now().toString()									//HEY make UID better
		, i = 0;
		
	//yield db.collection(UID).drop();	
		
	yield aggregateToCollection(db.collection("stoptimes"), [
		{$match:{stop_id:stop_id}},	
		{$lookup:{
			from: "trips",
			localField: "trip_id",
			foreignField: "trip_id",
			as: "trip_doc"
		}},
		{$project:{arrival_time:1, trip_id:1, stop_sequence:1, route_id:"$trip_doc.route_id", service_id:"$trip_doc.service_id"}},
		{$unwind:"$route_id"},
		{$unwind:"$service_id"},
		{$group:{
			
		}}
	], UID);
	
	console.log("got stop docs");
	
	var c = db.collection(UID).find();
	
	console.log(yield c.count());
	
	while (yield c.hasNext()){
		var doc = yield c.next();
		if (doc) yield db.collection(UID).updateOne({_id:doc._id},
			{$set:{time_to_wait: posMod(timeStr2Secs(doc.arrival_time)-now,3600*24)}}
		);
	}
	console.log("added time_to_wait fields");
	var next5 = db.collection(UID).aggregate([
		{$sort:{time_to_wait:1}},
		{$limit:5},
		//{$out:UID}
	]);
	
	console.dir(yield next5.toArray());
	//	db.collection(UID).drop();
	db.close();
});

getNextBus(requestStop, requestTime).catch(err=>{console.error(err.stack)});