import { MongoClient, ClientSession } from "mongodb";

export const lockUpdateTransaction = async (
    client: MongoClient,
    session: ClientSession,
) => {
    const lockColl = client.db("updates").collection("lock");
    await lockColl.findOneAndUpdate(
        {
            "in_transaction": {
                $exists: false,
            },
        },
        {
            $set: { "in_transaction": true },
        },
        {
            session,
            upsert: true,
        }
    );
}