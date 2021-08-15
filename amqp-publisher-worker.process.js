// producer

// client lib
import { connect } from 'amqplib/callback_api';

// credentials
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

export default () => {
  console.log('hello from', process.env.ROLE);

  process.on('message', (message) => {
    connect(credentials, function(err0, connection) {
      //todo: implement more error handling
      if (err0) throw err0;
      //create the channel from the connection
      connection.createChannel(function(err1, channel) {
        if (err1) throw err1;

        switch (message.event) {
          // direct
          case 'OTP.GENERATION':
            publishMessage(
              channel,
              message,
              process.env.EXCHANGE_DIRECT,
              process.env.EXCHANGE_DIRECT_TYPE,
            );
            break;
          // direct
          case 'USER.CREATION':
            publishMessage(
              channel,
              message,
              process.env.EXCHANGE_DIRECT,
              process.env.EXCHANGE_DIRECT_TYPE,
            );
            break;
          default:
            console.log('the event is not registered');
            break;
        }
        //close the connection
        setTimeout(function closeConnection() {
          connection.close();
        }, 500);
      });
    });
  });
};

//
function publishMessage(channel, message, exchangeName, type) {
  // setup the exchange
  const exchange = exchangeName; //? what name are mine using
  const event = {
    event: message.event,
    data: message.data,
  };
  channel.assertExchange(exchange, type, { durable: false });
  //publish the data to the exchange
  channel.publish(exchange, '', Buffer.from(JSON.stringify(event)));
  console.log('[X] sent %s', event);
  //TODO: what do i do after the message as been sent
}
//TODO: need to plane out the prduction implementation of this
// TODO: nned to implement a publishing method to handle verious event
