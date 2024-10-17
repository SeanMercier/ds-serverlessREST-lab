import { APIGatewayProxyHandlerV2 } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, GetCommand, QueryCommand } from "@aws-sdk/lib-dynamodb";

const ddbDocClient = createDocumentClient();

export const handler: APIGatewayProxyHandlerV2 = async (event, context) => {
  try {
    console.log("[EVENT]", JSON.stringify(event));

    const movieId = event.pathParameters?.movieId;
    if (!movieId) {
      return {
        statusCode: 400,
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({ message: "movieId path parameter is required" }),
      };
    }

    const includeCast = event.queryStringParameters?.cast === "true";

    // Fetch movie metadata
    const movieCommand = new GetCommand({
      TableName: process.env.MOVIES_TABLE_NAME,
      Key: { id: parseInt(movieId) },
    });
    const movieResult = await ddbDocClient.send(movieCommand);

    if (!movieResult.Item) {
      return {
        statusCode: 404,
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({ message: "Movie not found" }),
      };
    }

    const response = { movie: movieResult.Item };

    // If cast=true, fetch cast details from MovieCast table
    if (includeCast) {
      const castCommand = new QueryCommand({
        TableName: process.env.MOVIE_CAST_TABLE_NAME,
        KeyConditionExpression: "movieId = :movieId",
        ExpressionAttributeValues: {
          ":movieId": parseInt(movieId),
        },
      });
      const castResult = await ddbDocClient.send(castCommand);
      response["cast"] = castResult.Items || [];
    }

    return {
      statusCode: 200,
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify(response),
    };
  } catch (error: any) {
    console.error("Error retrieving movie data", error);
    return {
      statusCode: 500,
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({ message: "Internal Server Error", error: error.message }),
    };
  }
};

function createDocumentClient() {
  const ddbClient = new DynamoDBClient({ region: process.env.REGION });
  const marshallOptions = {
    convertEmptyValues: true,
    removeUndefinedValues: true,
    convertClassInstanceToMap: true,
  };
  const unmarshallOptions = {
    wrapNumbers: false,
  };
  const translateConfig = { marshallOptions, unmarshallOptions };
  return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}
