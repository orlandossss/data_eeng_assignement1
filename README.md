# data_eeng_assignement1

in the first get_user python script, the goal is to get all the datas from the api and put it in form.
There is a few things to do,  first we anonymized the users personal data such as name, email, dob, phone that we don't need (we replace by random values as we don't need them later).
Then we flatten all the datas going from json to a pd data frame so it can be easier to store in the database
Then we encrypt the data with a key (and store it preciously), the new data file can be now stored on minio

In the get_result_from_minio, we get the datas from minio, get the keys and then decrypt all the datas.
After getting all our datas decrypted, we can analysed them and got the important result (time since registration, gender repartition, biggest city/country in terms of connexions)

To connect, a config.json with all the credential is created and will be sent via a secure way
