var stomptClient = null;
var subscription = null;
var newQuery = 0;

function registerTemplate() {
	template = $("#template").html();
	Mustache.parse(template);
}

function setConnected(connected) {
	var search = $('#submitsearch');
	search.prop('disabled', !connected);
}

function connect() {
	var socket = new SockJS("/twitter");
	stompClient = Stomp.over(socket);
	stompClient.connect({}, function(frame) {
		setConnected(true);
		console.log('Connected: ' + frame);
	})
}

function registerSendQuery() {
	$("#search").submit(
			function(event) {
				event.preventDefault();
				if (subscription) {
					subscription.unsubscribe();
				}
				stompClient.send("/app/search", {}, $("#q").val());
				newQuery = 1;
				subscription = stompClient.subscribe("/queue/search/"
						+ $("#q").val(), function(data) {
					if (newQuery) {
						$("#resultsBlock").empty();
						newQuery = 0;
					}
					var tweet = JSON.parse(data.body);
					$("#resultsBlock")
							.prepend(Mustache.render(template, tweet));
				});
			});
}

$(document).ready(function() {
	registerTemplate();
	registerSendQuery();
	connect();
});
