const { Requester } = require("@chainlink/external-adapter");
const Rekognition = require("node-rekognition");
const https = require("https");
const Stream = require("stream").Transform;

const createRequest = async (input, callback) => {
  const AWSParameters = {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    region: process.env.AWS_REGION
  };

  const rekognition = new Rekognition(AWSParameters);

  return performRequest({
    input,
    callback,
    rekognition
  });
};

const performRequest = ({ input, callback, rekognition }) => {
  const { data, id: jobRunID } = input;

  if (!data) {
    callback(500, Requester.errored(jobRunID, "No data"));
    return;
  }

  const { hash } = data;

  if (jobRunID === undefined) {
    callback(500, Requester.errored(jobRunID, "Job run ID required"));
    return;
  }

  if (hash === undefined) {
    callback(500, Requester.errored(jobRunID, "Content hash required"));
  } else {
    const url = `https://${process.env.IPFS_GATEWAY_URL}/ipfs/${hash}`;

    try {
      https
        .request(url, function(response) {
          var imgBytesStream = new Stream();

          response.on("data", function(chunk) {
            imgBytesStream.push(chunk);
          });

          response.on("end", function() {
            requestModerationLabels(imgBytesStream.read());
          });
        })
        .end();

      const requestModerationLabels = async imgBytes => {
        try {
          const moderationLabels = await rekognition.detectModerationLabels(
            imgBytes
          );

          const convertedResult = convertLabelsToTrueSightFormat(
            moderationLabels.ModerationLabels
          );

          const response = {
            data: moderationLabels
          };

          response.data.result = convertedResult;

          callback(200, Requester.success(jobRunID, response));
        } catch (error) {
          console.error(error);
          callback(500, Requester.errored(jobRunID, error));
        }
      };
    } catch (error) {
      console.error(error);
      callback(500, Requester.errored(jobRunID, error));
    }
  }
};

const getConfidenceForLabel = (labelsData, labelToFind) => {
  const matchingLabels = labelsData.filter(
    label => label.Name.toLowerCase() === labelToFind.toLowerCase()
  );

  // Rekognition is able to determine all of the labels in our results format
  // so a missing label indicates some confidence the label does not exist.
  // => Return a 0 score instead of null
  if (!matchingLabels || matchingLabels.length === 0) {
    return 0;
  }

  return Math.round(parseFloat(matchingLabels[0].Confidence));
};

const convertLabelsToTrueSightFormat = labels => {
  const adultConfidence = getConfidenceForLabel(labels, "Explicit Nudity");
  const suggestiveConfidence = getConfidenceForLabel(labels, "Suggestive");
  const violenceConfidence = getConfidenceForLabel(labels, "Violence");
  const visuallyDisturbingConfidence = getConfidenceForLabel(labels, 
    "Visually Disturbing"
  );
  const hateSymbolsConfidence = getConfidenceForLabel(labels, "Hate Symbols");

  return [
    adultConfidence,
    suggestiveConfidence,
    violenceConfidence,
    visuallyDisturbingConfidence,
    hateSymbolsConfidence
  ].join(",");
};

// This is a wrapper to allow the function to work with
// GCP Functions
exports.gcpservice = (req, res) => {
  createRequest(req.body, (statusCode, data) => {
    res.status(statusCode).send(data);
  });
};

// This is a wrapper to allow the function to work with
// AWS Lambda
exports.handler = (event, context, callback) => {
  createRequest(event, (statusCode, data) => {
    callback(null, data);
  });
};

// This is a wrapper to allow the function to work with
// newer AWS Lambda implementations
exports.handlerv2 = (event, context, callback) => {
  createRequest(JSON.parse(event.body), (statusCode, data) => {
    callback(null, {
      statusCode: statusCode,
      body: JSON.stringify(data),
      isBase64Encoded: false
    });
  });
};

// This allows the function to be exported for testing
// or for running in express
module.exports.createRequest = createRequest;
module.exports.performRequest = performRequest;
