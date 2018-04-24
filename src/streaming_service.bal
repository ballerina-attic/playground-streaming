import ballerina/http;
import ballerina/io;

// Event streams and stream processing example!
// An 'event' is any occurrence that happens at a
// clearly defined time and is recorded in a collection of
// fields. 'Stream' is a constant flow of data events.

// Record type that represents the stock update event.
type StockUpdate {
    string symbol;
    float price;
};

// Record that represents the events that are produced after
// stream processing.
type Result {
    string symbol;
    int count;
    float average;
};

// Asynchronously invoke stream initialization function.
future f1 = start initStreamConsumer();

// Input stream constrained by ‘StockUpdate’ type.
// This stream receives and publishes incoming events.
stream<StockUpdate> inStream;

// Initializing the consumption of the input stream,
// specify the event processing logic and producing result
// to the output stream.
function initStreamConsumer () {

    // After the stream processing happens the resulted events
    // are sent to this stream.
    stream<Result> resultStream;

    // Subscribe event handler functions to the result event
    // stream. These handlers will be called when an event
    // is published to this stream.
    resultStream.subscribe(quoteCountEventHandler);
    resultStream.subscribe(quoteAverageEventHandler);

    // Never ending stream processing logic.
    // 'forever' clause consumes a stream of events and then
    // processes them in motion across a time window.

    // Within a time window of 3s, select the stock
    // quote events in which the stock price is larger than
    // 1000. Count the number of stock quotes that are
    // received during the time window for a given symbol and
    // calculate the average price of all such stock quotes.
    // The result is published to the resulstStream, which
    // triggers the function handlers for that stream stream.
    forever {
        from inStream where price > 1000
        window timeBatch(3000)
        select symbol
        , count(symbol) as count
        , avg(price) as average
        group by symbol
        => (Result [] result) {
            resultStream.publish(result);
        }
    }
}

// Registered as an event handlers for the resultStream.
// Prints out the stock ticker and increments count.
function quoteCountEventHandler (Result result) {
    io:println("Quote - " + result.symbol
            + " : count = " + result.count);
}

// Registered as an event handlers for the resultStream.
// Prints out the stock ticker and updates average value.
function quoteAverageEventHandler (Result result) {
    io:println("Quote - " + result.symbol
            + " : average = " + result.average);
}

endpoint http:Listener listener {
    port:9090
};

service<http:Service> nasdaq bind listener {

    publishQuote (endpoint conn, http:Request req) {
        string reqStr = check req.getTextPayload();
        float stockPrice  = check <float> reqStr;

        string stockSymbol = "GOOG";

        // Create a record from the content of the request.
        StockUpdate stockUpdate = {
            symbol:stockSymbol,
            price:stockPrice
        };

        // Publish the record as an event to the stream.
        // The forever block processes these events over time.
        inStream.publish(stockUpdate);

        http:Response res = new;
        res.statusCode = 202;
        _ = conn -> respond(res);
    }
}
