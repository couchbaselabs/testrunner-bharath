function OnUpdate(doc,meta) {
    var expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 5);
    var context = {docID : meta.id, random_text : "e6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh07Aumoe6cZZGHuh07Aumoe6cZZGHuh07Aumoe6"};

    createTimer(NDtimerCallback,  expiry, meta.id, context);
    createTimer(NDtimerCallback1,  expiry, meta.id, context);
}

function NDtimerCallback(context) {
    dst_bucket[context.docID] = context.random_text;
}

function NDtimerCallback1(context) {
    var query = UPSERT INTO dst_bucket1 ( KEY, VALUE ) VALUES ( UUID() ,'NDtimerCallback1');
//    query.execQuery();
}
