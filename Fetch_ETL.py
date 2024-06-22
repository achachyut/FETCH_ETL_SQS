import subprocess
import json
from datetime import datetime
import psycopg2

#########################################
##MASK THE PII SUCH AS IP AND DEVICE ID##
###READ THE JSON DATA IN THE AWS QUEUE###
#########################################
def mask_ip(ip):
    '''
    :param ip: ip is the Internet Protocol Address which will be in similar format of '123.123.123.123'
    :return: It will hide or mask the set of numbers before the 2nd '.'. Now, the IP Address looks like '***.***.123.123'
    '''
    new_ip= ip.split('.')
    return '.'.join(['*'*len(new_ip) if i<2 else x for i, x in enumerate(new_ip)])


def mask_device_id(device_id):
    '''

    :param device_id: device_id is the unique identification of the device which will be in the format of '123-45-6789'
    :return: It will hide or mask the set of numbers before the 2nd '-'. Now, the device_id looks like '***-**-6789'
    '''
    return '*'*(len(device_id)-4) +device_id[-4:]

def remove_decimal(app_version):
    '''
    :param app_version: app_version takes the number in similar format of '1.2.3'
    :return: It will remove the '.' so that the data could be inserted into postgres. Now, the new app_version value is '123'
    '''
    return app_version.replace('.', '')

def fetch_messages(total_messages):
    '''

    :param total_messages: Asks the user how many messages are wanted right now to view from the AWS sqs queue
    :return: Returns list of messages based on the user input
    '''
    #The following command interacts with local aws sqs and fetched the x number of messages from the specified queue hosted by Localstack
    command=f'docker exec -i localstack awslocal sqs receive-message --queue-url http://localhost:4566/000000000000/login-queue --max-number-of-messages {total_messages}'
    ##docker exec -it localstack awslocal sqs receive-message --queue-url http://localhost:4566/000000000000/login-queue


    try:
        # The following runs the CLI command provided above and then will load the json output
        result =subprocess.run(command,shell=True,capture_output=True,text=True,check=True)
        data=json.loads(result.stdout)

        if 'Messages' in data:
            return [(json.loads(message['Body']), message['ReceiptHandle']) for message in data['Messages']]

    except :
        print("Looks like there is no message in queue at the moment")


def process_message(message_body, receipt_handle):
    '''

    :param message_body:Inside each message is the 'Body' (i.e., message_body) which has the contents that our database needs.
    :param receipt_handle: There is 'ReceiptHandle' (i.e., receipt_handle) which is inside each message.
                            This receipt_handle is useful when once we have received the message and then we want to delete it.
    :return:    #Print & return the processed json message
    '''
     #The following are appropriate variable that need to inserted in the database

    json_message = {
        #Get the values from the body.
        #If the values are empty/unknown then write unknown or leav it blank. In case of app_version, write '0' for unkown values. In case of masked_ip, it will write '*' for it.
        "user_id": message_body.get('user_id','Unknown'),
        "device_type": message_body.get('device_type','Unknown'),
        ## The ip and device_id are the Personally Identifiable Information that are supposed to be hidden. Hence, mask_ip and mask_device_if will do the same
        "masked_ip": mask_ip(message_body.get('ip','')),
        "masked_device_id": mask_device_id(message_body.get('device_id','')),
        "locale": message_body.get('locale','Unknown'),
        "app_version": remove_decimal(message_body.get('app_version','0')),
        "create_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "receipt_handle": receipt_handle
    }



    print_message(json_message)
    return json_message


def print_message(message):
    '''

    :param message: The message dictinary is the input which has a similar format: message={"user_id":"1234", "device_type":"ios".... "create_date":"time"}
    :return: Returns the message in format :
    user_id : 12345
    device_type : ios
    .
    .
    .
    create_date: 2024-06-22
    '''
    for key,value in message.items():
        if key!='receipt_handle':
            print(f"{key}: {value}")
    print("---")



################################################
###CONNECTING AND WRITING MESSAGE TO POSTGRES###
################################################
def write_to_postgres(messages):
    '''

    :param messages: Takes list of messages as input
    :return: Returns whether or not the appropriate data was written to the postgres database or if there were any other errors
    '''

    try:
        #Connecting to the postgres database with the credentials
        conn=psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            password="postgres",
            port="5432"
        )
        cursor=conn.cursor()


        #The following query will write the outputted data to the postgres

        query="INSERT INTO user_logins (user_id,device_type,masked_ip, masked_device_id,locale,app_version,create_date) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        for message in messages:
            cursor.execute(query,(message['user_id'],message['device_type'],message['masked_ip'],message['masked_device_id'],message['locale'],message['app_version'],message['create_date']))

        conn.commit()
        print(f"Successfully inserted {len(messages)} data entry records in the database.")
        cursor.close()
        conn.close()

    #Catches database related error when writing to postgres otherwise close the connection
    except (Exception,psycopg2.Error) as error:
        print("Error while connecting to Postgres Database",error)
        return False
        cursor.close()
        conn.close()
    finally:
        if conn:
            cursor.close()
            conn.close()
    return True



def delete_message(receipt_handle):
    '''

    :param receipt_handle:  There is ReceiptHandle (i.e., receipt_handle) which is inside each message.
                            This receipt_handle is useful when once we have received the message and then we want to delete it. Hence, preventing duplication in the database.
    :return: Returns the status whether or not the expected entry was deleted from the queue
    '''
    #The following command interacts with local aws sqs and deletes the messages from the queue based on receipt_handle
    command= f'docker exec -i localstack awslocal sqs delete-message --queue-url http://localhost:4566/000000000000/login-queue --receipt-handle {receipt_handle}'

    try:
        subprocess.run(command, shell=True, check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error deleting message: {e}")
        return False


def main():
    print("*****************************\n"
          "Welcome to the Fetch ETL Tool\n"
          "******************************")
    while True:
        # Ask the user how many messages they want to see at a time and output that number of messages.
        while True:
            try:
                total_messages=int(input("\nHow many messages/records do you want to write to postgres at a time? "))
                if total_messages<=0:
                    print("Please enter a number greater than zero.")
                else:
                    raw_messages=  fetch_messages(total_messages)
                    break
            except ValueError:
                print("Invalid input. Please enter a valid number.")

        if not raw_messages:
            break
        processed_messages=[process_message(message, receipt_handle) for message, receipt_handle in raw_messages]

        #Choose one of the options
        choice=input("Choose the appropriate number (i.e., 1, 2, or 3) for next step? \n(1) See More Messages from AWS SQS"
                       "\n(2) Write these values to the Postgres Database"
                       "\n(3) Quit "
                       "\n")

        if choice=='2':
            if write_to_postgres(processed_messages):
                for message in processed_messages:
                    if delete_message(message['receipt_handle']):
                        print(f"Deleted message for user {message['user_id']} from the SQS queue.")
                    else:
                        print(f"Failed to delete message for user {message['user_id']} from the SQS queue.")
            break
        elif choice=='3':
            print("*****************************\n"
                  "Thank you for using this tool\n"
                  "*****************************")
            break
        elif choice!='1':
            print("Choose from 1, 2, or 3 please")


if __name__ == "__main__":
    main()