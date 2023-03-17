from kafka import KafkaConsumer, KafkaProducer
from json import loads, dumps
from statistics import mean, pstdev
# Initialize Kafka consumer and producer
consumer = KafkaConsumer(
    bootstrap_servers=['SERVER_URL'],
    auto_offset_reset='earliest')
consumer.subscribe(['energyprod'])
producer = KafkaProducer(
    bootstrap_servers=['SERVER_URL'],
    value_serializer=lambda x: dumps(x).encode('utf-8'))
# Initialize a list to store the last 8 hours of Phase_B_power values
window_size = 8 * 60 * 60 // 5  # 8 hours in 5-second intervals
print('window_size ' , window_size )


data = {}
# Read data stream from producer topic
for message in consumer:
    # print('data' , data)
    # print('len of power windows ', len(power_window))
    value_str = message.value.decode('utf-8')
    input_data = loads(value_str)
    print('value_str' , message)

    device_name = input_data['deviceName']
        # Extract the Phase_B_power value from the JSON object
    phase_b_power = input_data['Phase_B_power']
    
    if device_name not in data:
        data[device_name] = {'values': [], 'mean': None, 'stdev': None}

    data[device_name]['values'].append(phase_b_power)
    if len(data[device_name]['values']) > 8 * 60:  # Keep 8 hours of data
        data[device_name]['values'].pop(0)
    if len(data[device_name]['values']) >= 2:
        mean_val = mean(data[device_name]['values'])
        stdev_val = pstdev(data[device_name]['values'])
        if phase_b_power > mean_val + 2* stdev_val or phase_b_power < mean_val - 2* stdev_val:
            producer.send('energyc', input_data)
            print("Anomaly detected! Passing data to energyc topic." , phase_b_power)
        else:
            print("No anomaly detected.")
    

