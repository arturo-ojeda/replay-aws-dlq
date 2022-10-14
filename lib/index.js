/* eslint-disable no-unused-vars */
/* eslint-disable prettier/prettier */
const { Consumer } = require('sqs-consumer');
const Producer = require('sqs-producer');
const util = require('util');

const redrive = ({ from: sourceQueueUrl, to: destQueueUrl, attribute: attributeValue, options = {} }) => {
  const { sqs } = options;
  return new Promise((resolve, reject) => {
    const handleError = err => {
      // source.stop();      
      //reject(err);
      
    };

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

        if(!(hasAttibuteInMessageAttributes || hasAttributeInBody)) {
          console.log('❌ Message does not match: %s', message.MessageId);
          throw new Error('Message attribute does not match provided value. Ignoring message');          
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

      console.log('🔄 Replaying message: %s', message.MessageId);

      await send(payload);
      count++;
    };

    const source = Consumer.create({
      queueUrl: sourceQueueUrl,
      sqs,
      handleMessage
    });

    source.on('error', handleError);
    source.on('processing_error', handleError);
    source.on('message_processed', reportProcessed);
    source.on('empty', reportCompletion);

    source.start();
  });
};

module.exports = {
  redrive
};
