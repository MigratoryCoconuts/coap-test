const coap    = require('coap') // or coap
    , server  = coap.createServer()

server.on('request', function(req, res) {
  /*if (req.headers['Observe'] !== 0)
    return res.end(new Date().toISOString() + '\n')

  var interval = setInterval(function() {
    res.write(new Date().toISOString() + '\n')
  }, 1000)

  res.on('finish', function(err) {
    clearInterval(interval)
  })*/
	
	res.end(/*'Hello ' + req.url.split('/')[1] + '\nMessage payload:\n'+req.payload+'\n'*/);
	console.dir(res);
})

server.listen(function() {
  console.log('server started')
})