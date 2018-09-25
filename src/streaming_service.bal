import ballerina/http;
import ballerina/io;

// An 'event' is any occurrence that happens at a defined time and
// is recorded in a collection of fields. 'stream' is a constant
// flow of data events.

// Record type that represents the stock update event
type StockUpdate record {
    string symbol;
    float price;
};

// Represents the events that are produced after stream processing
type Result record {
    string symbol;
    int count;
    float average;
};

// Asynchronously invoke the stream initializer
future f1 = start initStreamConsumer();

// Receives and publishes incoming events
stream<StockUpdate> inStream;

// Initializes the input stream, contains event processing logic,
// and publishes events to an output stream.
function initStreamConsumer () {

    // Results after stream processing pushed to output stream 
    stream<Result> resultStream;

    // Event handler functions for events pushed to this stream
    resultStream.subscribe(quoteCountEventHandler);
    resultStream.subscribe(quoteAverageEventHandler);

    // Never-ending stream processing logic. A 'forever' clause
    // consumes events and processes them across a time window.

    // Within a time window of 3s, select the stock quote events in
    // which the stock price is larger than 1000. Count the number
    // of stock quotes that are received during the time window for
    // a given symbol and calculate the average price of all such
    // stock quotes. Publish the result to 'resultStream'.
    forever {
        from inStream where price > 1000
        window timeBatch(3000)
        select symbol,
            count(symbol) as count,
            avg(price) as average
        group by symbol
        => (Result [] result) {
            resultStream.publish(result);
        }
    }
}

// Event handler called for each event published by the processor
function quoteCountEventHandler (Result result) {
    io:println("Quote - " + result.symbol + 
        " : count = " + result.count);
}

// Event handler called for each event published by the processor
function quoteAverageEventHandler (Result result) {
    io:println("Quote - " + result.symbol + 
        " : average = " + result.average);
}

endpoint http:Listener listener {
    port:9090
};

// This service is the program's entrypoint. Each invocation to 
// this service will generate an event to be handled by the stream
// processor as part of its time window analysis.
service<http:Service> nasdaq bind listener {

    publishQuote (endpoint conn, http:Request req) {
        string reqStr = check req.getTextPayload();
        float stockPrice  = check <float> reqStr;

        string stockSymbol = "GOOG";

        // Create a record from the content of the request
        StockUpdate stockUpdate = {
            symbol:stockSymbol,
            price:stockPrice
        };

        // Publishan event to the stream managed by the processor
        inStream.publish(stockUpdate);

        http:Response res = new;
        res.statusCode = 202;
        _ = conn -> respond(res);
    }
}
