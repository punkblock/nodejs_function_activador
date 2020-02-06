'use strict';
var AWS = require("aws-sdk");
var _ = require("lodash");
var docClient = new AWS.DynamoDB.DocumentClient();

exports.handler = async function(event, context) {
  AWS.config.update({
    region: "us-east-1",
    endpoint: "https://dynamodb.us-east-1.amazonaws.com"
  });

  console.log("Querying for data by ID.");

  var allData = [];
  var allCount = [];
  // sensors => # quantity sensor
  var sensors = 2
  var algo = [1, 2, 3];
  console.log("Begin forEach")
  _.forEach([1, 2, 3], function(id) {
      console.log("in forEach")
        // filter
      var params = {
        TableName: "data_save",
        KeyConditionExpression: "#ID = :id",
        ExpressionAttributeNames: {
          "#ID": "ID"
        },
        ExpressionAttributeValues: {
          ":id": id
        }
      };

      //query
      console.log('Before Algo')
      docClient.query(params, async function(err, data) {
        // error
        console.log('After Algoo')
        if (err) {
          console.error("YGYTUUYTI Error:", JSON.stringify(err, null, 2));
        } else {
          console.log("Query succeeded.");
          // push data
          allData.push(data);
          // push counter message
          allCount.push(data.Count);
          // print info
          // data.Items.forEach(function(item) {
          //   console.log(" -", item.ID + ": " + item.Payload.tem);
          // });
        }
        // validate that all sensors have sent data at least once
        if (allData.length == sensors) {
          // all sensors has send equal data cuantity
          if (_.uniqBy(allCount).length == 1) {
            var getMean = [];
            var mean = 0;
            _.forEach(allData, async function(item) {
              var messageLength = (_.findLastKey(item.Items, 'Payload'));
              // console.log(item.Items[messageLength].Payload.tem);
              getMean.push(item.Items[messageLength].Payload.tem);
            });
            console.log('ppppp', getMean)
            mean = _.mean(getMean)
            console.log('Mean: ', mean)
            if (mean > 24)  {
              console.log('yai')
                //publish in aws topic: iot/proteinlab/location/invernalab/actuador
              var iotdata = new AWS.IotData({ endpoint: 'ahty8o07ilwa-ats.iot.us-east-1.amazonaws.com' });

              var params = {
                topic: 'iot/proteinlab/location/invernalab/actuador',
                payload: 'Condiciones cumplidas',
                qos: 0
              };

              return iotdata.publish(params, async function(err, data) {
                if (err) {
                  console.log(err);
                } else {
                  console.log("Success, I guess.");
                  console.log("EVENT: \n" + JSON.stringify(event, null, 2))
                    // return context.logStreamName
                    //context.succeed();
                }
              });
            }
          }
          // console.log('ADATA: ', allData)
          // console.log('Couuunt: ', _.uniqBy(allCount).length, allCount)
        }
      });
    }

  )
}