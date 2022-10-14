/* eslint-disable no-unused-vars */
/* eslint-disable prettier/prettier */
const { Consumer } = require('sqs-consumer');
const Producer = require('sqs-producer');
const util = require('util');
var debounce = require('debounce');

const redrive = ({ from: sourceQueueUrl, to: destQueueUrl, attribute: attributeValue, options = {} }) => {
  const { sqs } = options;
  return new Promise((resolve, reject) => {
    const handleError = err => {
      // source.stop();      
      //reject(err);
      
    };

    const noMoreMessagesReceivedInLast2Seconds = debounce(() => {
      console.log('🟡 No more messages received in last 2 seconds. Stop the app by pressing CTRL+C or filtered messages will be received again.');
      source.stop();
      resolve();
    }, 2000);


    const reportProcessed = message => {
      
    };

    let count = 0;
    const isFIFO = /\.fifo$/.test(destQueueUrl);
    
    const target = Producer.create({
      sqs,
      queueUrl: destQueueUrl
    });
    
    const send = util.promisify(target.send.bind(target));

    const reportCompletion = () => {
      source.stop();
      console.log(`🟢 Replayed ${count} message(s) on: ${destQueueUrl}`);
      resolve();
    };

    const handleMessage = async message => {
      let payload = {
        id: message.MessageId,
        body: message.Body,
        messageAttributes: message.MessageAttributes
      };

      if (attributeValue !== undefined) {        
        const hasAttibuteInMessageAttributes = message.MessageAttributes !== undefined && message.MessageAttributes['beanstalk.sqsd.task_name'] !== undefined && message.MessageAttributes['beanstalk.sqsd.task_name'].StringValue === attributeValue;
        const hasAttributeInBody = message.Body && JSON.parse(message.Body).jobType === attributeValue;
        
        const jobType = JSON.parse(message.Body).jobType ? JSON.parse(message.Body).jobType : message.MessageAttributes['beanstalk.sqsd.task_name'].StringValue;

        if(!(hasAttibuteInMessageAttributes || hasAttributeInBody)) {
          console.log(`❌ Message does not match (${jobType}): %s`, message.MessageId);
          noMoreMessagesReceivedInLast2Seconds();
          throw new Error(`Message attribute does not match provided value (${jobType}). Ignoring message`);          
        } 
      }

      // For FIFO queue we need to make sure this message is unique and is in correct order
      if (isFIFO) {
        payload = {
          ...payload,
          groupId: 're-drive',
          deduplicationId: `${message.MessageId}_${Date.now()}`
        };
      }      

      console.log(`🔄 Replaying message (${attributeValue}) : %s`, message.MessageId);

      await send(payload);
      count++;
      noMoreMessagesReceivedInLast2Seconds();
    };

    const source = Consumer.create({
      queueUrl: sourceQueueUrl,
      sqs,
      handleMessage
    });

    source.on('error', handleError);
    source.on('processing_error', handleError);
    source.on('message_processed', reportProcessed);
    // source.on('empty', reportCompletion);

    source.start();
  });
};

module.exports = {
  redrive
};
