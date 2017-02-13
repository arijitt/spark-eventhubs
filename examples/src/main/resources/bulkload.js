function bulkImport(batchId, upsert) {
    var collection = getContext().getCollection();
    var collectionLink = collection.getSelfLink();

    var errorCodes = { CONFLICT: 409 };

    var query = "select * from root r where r.batchId = '" + batchId + "'";

    var isAccepted = collection.queryDocuments(collectionLink, query, {}, {});

    if (!isAccepted) getContext().getResponse().setBody(count);




    function retrieveDoc(doc, continuation, callback) {
        var query = "select * from root r where r.batchId = '" + doc.batchId + "'";
        var requestOptions = { continuation : continuation };
        var isAccepted = collection.queryDocuments(collectionLink, query, requestOptions, function(err, retrievedDocs, responseOptions) {
            if (err) throw err;

            if (retrievedDocs.length > 0) {
                callback(retrievedDocs);
            } else if (responseOptions.continuation) {
                retrieveDoc(doc, responseOptions.continuation, callback);
            } else {
                throw "Error in retrieving document: " + doc.id;
            }
        });

        if (!isAccepted) getContext().getResponse().setBody(count);
    }
 }