const coap    = require('coap') // or coap
    , server  = coap.createServer()
		, readDb 	= require('./getNextBus.js')
		, co 			= require("co");
		
var handleRequest = co.wrap(function*(req, res) {
	 //if (req.headers['Observe'] !== 0)
		
	try {
		var stop = req.url.split('/')[1]
			, now = new Date();
		console.log(`Incoming request for stop ${stop}`);
		var isValid = yield readDb.isValidStop(stop)
		console.log(`${stop} is ${isValid?"":"not"} a valid stop`);
		
		if (isValid) {
			var nextBuses = yield readDb.getNextBus(stop, now);
			res.setOption('Content-Format', 'application/json');
			res.write(JSON.stringify(nextBuses))
			res.end(() => {console.log(`Responded with data after ${Date.now() - now.valueOf()} ms`)});
		} else {
			res.code = 404;
			res.end(() => console.log(`Responded with 404`));
		}
	} catch(err) {
		res.code = 500;
		console.trace(err);
		res.end(() => console.log(`There was an error! responded with 500`));
	}
});

server.on('request', handleRequest);

server.listen(function() {
  console.log('server started')
})