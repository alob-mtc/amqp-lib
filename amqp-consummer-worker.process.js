// consummer
// import AMQP lib
import { connect } from 'amqplib/callback_api';
import { createHash } from 'crypto';
// credentials object
const credentials = {
  protocol: 'amqp',
  hostname: process.env.AMQP_HOSTNAME,
  username: process.env.AMQP_USERNAME,
  password: process.env.AMQP_PASSWORD,
  locale: 'en_US',
  frameMax: 0,
  heartbeat: 0,
  vhost: '/',
};
let done = false;
let success;
let requeue;
export default () => {
  console.log('hello from', process.env.ROLE);

  // register a listener for the consummer process
  //* the worker process send ack to the consummer process the it is done processing the work
  process.on('message', (message) => {
    if (message.done === true) {
      success = message.event === 'TASK.COMPLETE' ? true : false;
      requeue = message.event === 'TASK.COMPLETE' ? false : message.requeue;
      done = true;
    }
  });
  // connect to the rabbitMQ server
  connect(credentials, (err0, connection) => {
    //gracefully close connection before process exits
    process.once('SIGINT', () => connection.close());
    process.once('exit', () => connection.close());
    if (err0) throw err0;
    // create a channel with the connection
    connection.createChannel((err1, channel) => {
      if (err1) throw err1;
      const queue = process.env.CONSUMMER_QUEUE;
      const exchange = process.env.CONSUMMER_EXCHANGE;
      // insert the exchange if it does not exist yet
      //THE PUBLISHER SERVICE DETERMINES THE EXCHANGE type
      channel.assertExchange(exchange, process.env.CONSUMMER_EXCHANGE_TYPE, {
        durable: false,
      });
      // the queue should be durable
      channel.assertQueue(queue, { durable: true });
      // bind the queue to the exchange
      channel.bindQueue(queue, exchange, '');
      console.log('[X] waiting for message in %s', queue);
      channel.prefetch(1); //consumme on message at a time
      channel.consume(
        queue,
        (msg) => {
          try {
            console.log(
              '[X] Received %s %d',
              JSON.parse(msg.content.toString()),
            );
            sendMessage(msg, channel);
            const end = function() {
              if (done === true) {
                success
                  ? channel.ack(msg)
                  : // if the error that caused the task to fail is 100% likly to occur again => do not requeue the message
                  requeue
                  ? channel.nack(msg)
                  : channel.reject(msg, false); // remove the messge from the queue => do not requeue
                done = false;
                return;
              }
              setTimeout(end, 1000);
            };
            end();
          } catch (error) {
            console.log(error.message);
          }
        },
        { noAck: false },
      );
    });
  });
};
// send message function
function sendMessage(msg, channel) {
  const newMessage = JSON.parse(msg.content.toString());
  // TODO: validate the msg
  if (
    !newMessage.hasOwnProperty('event') ||
    !newMessage.hasOwnProperty('data')
  ) {
    // remove the message from the queue and retrun if it is not a valid format
    channel.reject(msg, false);
    throw new Error('the message recieved is an ivalid data');
  }
  process.send(newMessage);
}

// the hashing function
function makeHash(text) {
  return createHash('md5')
    .update(text, 'utf-8')
    .digest('hex');
}
//TODO: nee to plane out the prduction implementation of this
//TODO: need to requeue the messgae if the worker task return failed => done
// TODO : if the consummer failes to consumme a massage more than a give limit aert error
//! if the task faild becuse of the wallet is frozen the task get removed from the queue without been processed approprately
