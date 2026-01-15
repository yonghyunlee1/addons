import paho.mqtt.client as mqtt
import json
import time
import asyncio
import threading
import telnetlib
import socket
import random

from threading import Thread
from queue import Queue

# DEVICE 별 패킷 정보
RS485_DEVICE = {
    'light': {
        'state':    { 'id': '0E', 'cmd': '81' },

        'power':    { 'id': '0E', 'cmd': '41', 'ack': 'C1' }
    },
    'thermostat': {
        'state':    { 'id': '36', 'cmd': '81' },
        
        'power':    { 'id': '36', 'cmd': '43', 'ack': 'C3' },
        'away':    { 'id': '36', 'cmd': '45', 'ack': 'C5' },
        'target':   { 'id': '36', 'cmd': '44', 'ack': 'C4' }
    },
    'plug': {
#asis   'state':    { 'id': '50', 'cmd': '81' },
        'state':    { 'id': '39', 'cmd': '81' },

#asis   'power':    { 'id': '50', 'cmd': '43', 'ack': 'C3' }
        'power':    { 'id': '39', 'cmd': '41', 'ack': 'C1' }
    },
    'gasvalve': {
        'state':    { 'id': '12', 'cmd': '81' },

        'power':    { 'id': '12', 'cmd': '41', 'ack': 'C1' } # 잠그기만 가능
    },
    'fan': {
        'state': { 'id': '32', 'cmd': '81' },

        'power': { 'id': '32', 'cmd': '41', 'ack': 'C1' },
        'speed': { 'id': '32', 'cmd': '42', 'ack': 'C2' },
        'mode':  { 'id': '32', 'cmd': '43', 'ack': 'C3' }
    },
    'batch': {
        'state':    { 'id': '33', 'cmd': '81' },

        'press':    { 'id': '33', 'cmd': '41', 'ack': 'C1' }
    }
}

# MQTT Discovery를 위한 Preset 정보
DISCOVERY_DEVICE = {
    'ids': ['ezville_wallpad',],
    'name': 'ezville_wallpad',
    'mf': 'EzVille',
    'mdl': 'EzVille Wallpad',
    'sw': 'ktdo79/addons/ezville_wallpad',
}

# MQTT Discovery를 위한 Payload 정보
DISCOVERY_PAYLOAD = {
    'light': [ {
        '_intg': 'light',
        '~': 'ezville/light_{:0>2d}_{:0>2d}',
        'name': 'ezville_light_{:0>2d}_{:0>2d}',
        'opt': True,
        'stat_t': '~/power/state',
        'cmd_t': '~/power/command'
    } ],
    'thermostat': [ {
        '_intg': 'climate',
        '~': 'ezville/thermostat_{:0>2d}_{:0>2d}',
        'name': 'ezville_thermostat_{:0>2d}_{:0>2d}',
        'mode_cmd_t': '~/power/command',
        'mode_stat_t': '~/power/state',
        'temp_stat_t': '~/setTemp/state',
        'temp_cmd_t': '~/setTemp/command',
        'curr_temp_t': '~/curTemp/state',
#        "modes": [ "off", "heat", "fan_only" ],     # 외출 모드는 fan_only로 매핑
        'modes': [ 'heat', 'off' ],     # 외출 모드는 off로 매핑
        'min_temp': '5',
        'max_temp': '40'
    } ],
    'plug': [ {
        '_intg': 'switch',
        '~': 'ezville/plug_{:0>2d}_{:0>2d}',
        'name': 'ezville_plug_{:0>2d}_{:0>2d}',
        'stat_t': '~/power/state',
        'cmd_t': '~/power/command',
        'icon': 'mdi:leaf'
    },
    {
        '_intg': 'binary_sensor',
        '~': 'ezville/plug_{:0>2d}_{:0>2d}',
        'name': 'ezville_plug-automode_{:0>2d}_{:0>2d}',
        'stat_t': '~/auto/state',
        'icon': 'mdi:leaf'
    },
    {
        '_intg': 'sensor',
        '~': 'ezville/plug_{:0>2d}_{:0>2d}',
        'name': 'ezville_plug_{:0>2d}_{:0>2d}_powermeter',
        'stat_t': '~/current/state',
        'unit_of_meas': 'W'
    } ],
    'gasvalve': [ {
        '_intg': 'switch',
        '~': 'ezville/gasvalve_{:0>2d}_{:0>2d}',
        'name': 'ezville_gasvalve_{:0>2d}_{:0>2d}',
        'stat_t': '~/power/state',
        'cmd_t': '~/power/command',
        'icon': 'mdi:valve'
    } ],
    'fan': [ {
        '_intg': 'fan',
        '~': 'ezville/fan_{:0>2d}_{:0>2d}',
        'name': 'ezville_fan_{:0>2d}_{:0>2d}',
    
        'stat_t': '~/state',
        'cmd_t': '~/command',
    
        'percentage_stat_t': '~/percentage/state',
        'percentage_cmd_t': '~/percentage/command',
    
        'preset_mode_stat_t': '~/preset_mode/state',
        'preset_mode_cmd_t': '~/preset_mode/command',
        'preset_modes': ['bypass', 'energysaving'],
        
        'icon': 'mdi:fan'
    } ],
    'batch': [ {
        '_intg': 'button',
        '~': 'ezville/batch_{:0>2d}_{:0>2d}',
        'name': 'ezville_batch-elevator-up_{:0>2d}_{:0>2d}',
        'cmd_t': '~/elevator-up/command',
        'icon': 'mdi:elevator-up'
    },
    {
        '_intg': 'button',
        '~': 'ezville/batch_{:0>2d}_{:0>2d}',
        'name': 'ezville_batch-elevator-down_{:0>2d}_{:0>2d}',
        'cmd_t': '~/elevator-down/command',
        'icon': 'mdi:elevator-down'
    },
    {
        '_intg': 'binary_sensor',
        '~': 'ezville/batch_{:0>2d}_{:0>2d}',
        'name': 'ezville_batch-groupcontrol_{:0>2d}_{:0>2d}',
        'stat_t': '~/group/state',
        'icon': 'mdi:lightbulb-group'
    },
    {
        '_intg': 'binary_sensor',
        '~': 'ezville/batch_{:0>2d}_{:0>2d}',
        'name': 'ezville_batch-outing_{:0>2d}_{:0>2d}',
        'stat_t': '~/outing/state',
        'icon': 'mdi:home-circle'
    } ]
}

# STATE 확인용 Dictionary
STATE_HEADER = {
    prop['state']['id']: (device, prop['state']['cmd'])
    for device, prop in RS485_DEVICE.items()
    if 'state' in prop
}

# ACK 확인용 Dictionary
ACK_HEADER = {
    prop[cmd]['id']: (device, prop[cmd]['ack'])
    for device, prop in RS485_DEVICE.items()
        for cmd, code in prop.items()
            if 'ack' in code
}

# LOG 메시지
def log(string):
    date = time.strftime('%Y-%m-%d %p %I:%M:%S', time.localtime(time.time()))
    print('[{}] {}'.format(date, string))
    return

# CHECKSUM 및 ADD를 마지막 4 BYTE에 추가
def checksum(input_hex):
    try:
        input_hex = input_hex[:-4]
        
        # 문자열 bytearray로 변환
        packet = bytes.fromhex(input_hex)
        
        # checksum 생성
        checksum = 0
        for b in packet:
            checksum ^= b
        
        # add 생성
        add = (sum(packet) + checksum) & 0xFF 
        
        # checksum add 합쳐서 return
        return input_hex + format(checksum, '02X') + format(add, '02X')
    except:
        return None

    
config_dir = '/data'

HA_TOPIC = 'ezville'
STATE_TOPIC = HA_TOPIC + '/{}/{}/state'
EW11_TOPIC = 'ew11'
EW11_SEND_TOPIC = EW11_TOPIC + '/send'


# Main Function
def ezville_loop(config):
    
    # Log 생성 Flag
    debug = config['DEBUG_LOG']
    mqtt_log = config['MQTT_LOG']
    ew11_log = config['EW11_LOG']
    
    # 통신 모드 설정: mixed, socket, mqtt
    comm_mode = config['mode']
    
    # Socket 정보
    SOC_ADDRESS = config['ew11_server']
    SOC_PORT = config['ew11_port']
    
    # EW11 혹은 HA 전달 메시지 저장소
    MSG_QUEUE = Queue()
    
    # EW11에 보낼 Command 및 예상 Acknowledge 패킷 
    CMD_QUEUE = asyncio.Queue()
    
    # State 저장용 공간
    DEVICE_STATE = {}
    
    # 이전에 전달된 패킷인지 판단을 위한 캐쉬
    MSG_CACHE = {}
    
    # MQTT Discovery Que
    DISCOVERY_DELAY = config['discovery_delay']
    DISCOVERY_LIST = []
    
    # EW11 전달 패킷 중 처리 후 남은 짜투리 패킷 저장
    RESIDUE = ''
    
    # 강제 주기적 업데이트 설정 - 매 force_update_period 마다 force_update_duration초간 HA 업데이트 실시
    FORCE_UPDATE = False
    FORCE_MODE = config['force_update_mode']
    FORCE_PERIOD = config['force_update_period']
    FORCE_DURATION = config['force_update_duration']
    
    # Command를 EW11로 보내는 방식 설정 (동시 명령 횟수, 명령 간격 및 재시도 횟수)
    CMD_INTERVAL = config['command_interval']
    CMD_RETRY_COUNT = config['command_retry_count']
    FIRST_WAITTIME = config['first_waittime']
    RANDOM_BACKOFF = config['random_backoff']
    
    # State 업데이트 루프 / Command 실행 루프 / Socket 통신으로 패킷 받아오는 루프 / Restart 필요한지 체크하는 루프의 Delay Time 설정
    STATE_LOOP_DELAY = config['state_loop_delay']
    COMMAND_LOOP_DELAY = config['command_loop_delay']
    SERIAL_RECV_DELAY = config['serial_recv_delay']
    RESTART_CHECK_DELAY = config['restart_check_delay']
    
    # EW11에 설정된 BUFFER SIZE
    EW11_BUFFER_SIZE = config['ew11_buffer_size']
    
    # EW11 동작상태 확인용 메시지 수신 시간 체크 주기 및 체크용 시간 변수
    EW11_TIMEOUT = config['ew11_timeout']
    last_received_time = time.time()
    
    # EW11 재시작 확인용 Flag
    restart_flag = False
  
    # MQTT Integration 활성화 확인 Flag - 단, 사용을 위해서는 MQTT Integration에서 Birth/Last Will Testament 설정 및 Retain 설정 필요
    MQTT_ONLINE = False
    
    # Addon 정상 시작 Flag
    ADDON_STARTED = False
 
    # Reboot 이후 안정적인 동작을 위한 제어 Flag
    REBOOT_CONTROL = config['reboot_control']
    REBOOT_DELAY = config['reboot_delay']

    # elevator call 반복횟수 제어 yh
    EVEVATOR_CALL_DELAY = config['elevator_call_delay']
    EVEVATOR_CALL_CNT = config['elevator_call_cnt']
    
    # 시작 시 인위적인 Delay 필요시 사용
    startup_delay = 0
  

    # MQTT 통신 연결 Callback
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            log('[INFO] MQTT Broker 연결 성공')
            # Socket인 경우 MQTT 장치의 명령 관련과 MQTT Status (Birth/Last Will Testament) Topic만 구독
            if comm_mode == 'socket':
                client.subscribe([(HA_TOPIC + '/#', 0), ('homeassistant/status', 0)])
            # Mixed인 경우 MQTT 장치 및 EW11의 명령/수신 관련 Topic 과 MQTT Status (Birth/Last Will Testament) Topic 만 구독
            elif comm_mode == 'mixed':
                client.subscribe([(HA_TOPIC + '/#', 0), (EW11_TOPIC + '/recv', 0), ('homeassistant/status', 0)])
            # MQTT 인 경우 모든 Topic 구독
            else:
                client.subscribe([(HA_TOPIC + '/#', 0), (EW11_TOPIC + '/recv', 0), (EW11_TOPIC + '/send', 1), ('homeassistant/status', 0)])
        else:
            errcode = {1: 'Connection refused - incorrect protocol version',
                       2: 'Connection refused - invalid client identifier',
                       3: 'Connection refused - server unavailable',
                       4: 'Connection refused - bad username or password',
                       5: 'Connection refused - not authorised'}
            log(errcode[rc])
         
        
    # MQTT 메시지 Callback
    def on_message(client, userdata, msg):
        nonlocal MSG_QUEUE
        nonlocal MQTT_ONLINE
        nonlocal startup_delay
        
        if msg.topic == 'homeassistant/status':
            # Reboot Control 사용 시 MQTT Integration의 Birth/Last Will Testament Topic은 바로 처리
            if REBOOT_CONTROL:
                status = msg.payload.decode('utf-8')
                
                if status == 'online':
                    log('[INFO] MQTT Integration 온라인')
                    MQTT_ONLINE = True
                    if not msg.retain:
                        log('[INFO] MQTT Birth Message가 Retain이 아니므로 정상화까지 Delay 부여')
                        startup_delay = REBOOT_DELAY
                elif status == 'offline':
                    log('[INFO] MQTT Integration 오프라인')
                    MQTT_ONLINE = False
        # 나머지 topic은 모두 Queue에 보관
        else:
            MSG_QUEUE.put(msg)
 

    # MQTT 통신 연결 해제 Callback
    def on_disconnect(client, userdata, rc):
        log('INFO: MQTT 연결 해제')
        pass


    # MQTT message를 분류하여 처리
    async def process_message():
        # MSG_QUEUE의 message를 하나씩 pop
        nonlocal MSG_QUEUE
        nonlocal last_received_time
        
        stop = False
        while not stop:
            if MSG_QUEUE.empty():
                stop = True
            else:
                msg = MSG_QUEUE.get()
                topics = msg.topic.split('/')

                if topics[0] == HA_TOPIC and topics[-1] == 'command':
                    await HA_process(topics, msg.payload.decode('utf-8'))
                elif topics[0] == EW11_TOPIC and topics[-1] == 'recv':
                    # Que에서 확인된 시간 기준으로 EW11 Health Check함.
                    last_received_time = time.time()

                    await EW11_process(msg.payload.hex().upper())
                   
    
    # EW11 전달된 메시지 처리
    async def EW11_process(raw_data):
        nonlocal DISCOVERY_LIST
        nonlocal RESIDUE
        nonlocal MSG_CACHE
        nonlocal DEVICE_STATE       
        
        raw_data = RESIDUE + raw_data
        
        if ew11_log:
            log('[SIGNAL] receved: {}'.format(raw_data))
        
        k = 0
        cors = []
        msg_length = len(raw_data)
        while k < msg_length:
            # F7로 시작하는 패턴을 패킷으로 분리
            if raw_data[k:k + 2] == 'F7':
                # 남은 데이터가 최소 패킷 길이를 만족하지 못하면 RESIDUE에 저장 후 종료
                if k + 10 > msg_length:
                    RESIDUE = raw_data[k:]
                    break
                else:
                    data_length = int(raw_data[k + 8:k + 10], 16)
                    packet_length = 10 + data_length * 2 + 4 
                    
                    # 남은 데이터가 예상되는 패킷 길이보다 짧으면 RESIDUE에 저장 후 종료
                    if k + packet_length > msg_length:
                        RESIDUE = raw_data[k:]
                        break
                    else:
                        packet = raw_data[k:k + packet_length]
                        
                # 분리된 패킷이 Valid한 패킷인지 Checksum 확인                
                if packet != checksum(packet):
                    k+=1
                    continue
                else:
                    STATE_PACKET = False
                    ACK_PACKET = False
                    
                    # STATE 패킷인지 확인
                    if packet[2:4] in STATE_HEADER and packet[6:8] in STATE_HEADER[packet[2:4]][1]:
                        STATE_PACKET = True
                    # ACK 패킷인지 확인
                    elif packet[2:4] in ACK_HEADER and packet[6:8] in ACK_HEADER[packet[2:4]][1]:
                        ACK_PACKET = True
                    
                    if STATE_PACKET or ACK_PACKET:
                        # MSG_CACHE에 없는 새로운 패킷이거나 FORCE_UPDATE 실행된 경우만 실행
                        if MSG_CACHE.get(packet[0:10]) != packet[10:] or FORCE_UPDATE:
                            name = STATE_HEADER[packet[2:4]][0]                            
                            if name == 'light':
                                # ROOM ID
                                rid = int(packet[5], 16)
                                # ROOM의 light 갯수 + 1
                                slc = int(packet[8:10], 16) 
                                
                                for id in range(1, slc):
                                    discovery_name = '{}_{:0>2d}_{:0>2d}'.format(name, rid, id)
                                    
                                    if discovery_name not in DISCOVERY_LIST:
                                        DISCOVERY_LIST.append(discovery_name)
                                    
                                        payload = DISCOVERY_PAYLOAD[name][0].copy()
                                        payload['~'] = payload['~'].format(rid, id)
                                        payload['name'] = payload['name'].format(rid, id)
                                   
                                        # 장치 등록 후 DISCOVERY_DELAY초 후에 State 업데이트
                                        await mqtt_discovery(payload)
                                        await asyncio.sleep(DISCOVERY_DELAY)
                                    
                                    # State 업데이트까지 진행
                                    onoff = 'ON' if int(packet[10 + 2 * id: 12 + 2 * id], 16) > 0 else 'OFF'

                                    #디밍조명 예외처리 yh
                                    if rid == 1:
                                        if id == 1 or id == 2:
                                            if int(packet[10 + 2 * id: 12 + 2 * id], 16) > 6:
                                                onoff = 'ON'
                                            else: 
                                                onoff = 'OFF'
                                    elif rid == 2 or rid == 3 or rid == 4 or rid == 5:
                                        if int(packet[10 + 2 * id: 12 + 2 * id], 16) > 6:
                                            onoff = 'ON'
                                        else: 
                                            onoff = 'OFF'

                                    #log('[YH] ->> onoff : {} [{}] >> {} {} %%% {}'.format(onoff, int(packet[10 + 2 * id: 12 + 2 * id], 16), rid, id, packet))
                                        
                                    await update_state(name, 'power', rid, id, onoff)
                                    
                                    # 직전 처리 State 패킷은 저장
                                    if STATE_PACKET:
                                        MSG_CACHE[packet[0:10]] = packet[10:]
                                                                                    
                            elif name == 'thermostat':
                                # room 갯수
                                rc = int((int(packet[8:10], 16) - 5) / 2)
                                # room의 조절기 수 (현재 하나 뿐임)
                                src = 1
                                
                                onoff_state = bin(int(packet[12:14], 16))[2:].zfill(8)
                                away_state = bin(int(packet[14:16], 16))[2:].zfill(8)
                                
                                for rid in range(1, rc + 1):
                                    discovery_name = '{}_{:0>2d}_{:0>2d}'.format(name, rid, src)
                                    
                                    if discovery_name not in DISCOVERY_LIST:
                                        DISCOVERY_LIST.append(discovery_name)
                                    
                                        payload = DISCOVERY_PAYLOAD[name][0].copy()
                                        payload['~'] = payload['~'].format(rid, src)
                                        payload['name'] = payload['name'].format(rid, src)
                                   
                                        # 장치 등록 후 DISCOVERY_DELAY초 후에 State 업데이트
                                        await mqtt_discovery(payload)
                                        await asyncio.sleep(DISCOVERY_DELAY)
                                    
                                    setT = str(int(packet[16 + 4 * rid:18 + 4 * rid], 16))
                                    curT = str(int(packet[18 + 4 * rid:20 + 4 * rid], 16))
                                    
                                    if onoff_state[8 - rid ] == '1':
                                        onoff = 'heat'
                                    # 외출 모드는 off로 
                                    elif onoff_state[8 - rid] == '0' and away_state[8 - rid] == '1':
                                        onoff = 'off'
#                                    elif onoff_state[8 - rid] == '0' and away_state[8 - rid] == '0':
#                                        onoff = 'off'
#                                    else:
#                                        onoff = 'off'

                                    await update_state(name, 'power', rid, src, onoff)
                                    await update_state(name, 'curTemp', rid, src, curT)
                                    await update_state(name, 'setTemp', rid, src, setT)
                                    
                                # 직전 처리 State 패킷은 저장
                                if STATE_PACKET:
                                    MSG_CACHE[packet[0:10]] = packet[10:]
                                else:
                                    # Ack 패킷도 State로 저장
                                    MSG_CACHE['F7361F810F'] = packet[10:]
                                        
                            # plug는 ACK PACKET에 상태 정보가 없으므로 STATE_PACKET만 처리
                            elif name == 'plug' and STATE_PACKET:
                                #log('[YH] ->> TEST(plug state) : [{}]'.format(packet))
                                if STATE_PACKET:
                                    # ROOM ID
                                    #yh rid = int(packet[5], 16)
                                    rid = int(packet[4], 16)
                                    # ROOM의 plug 갯수
                                    #yh spc = int(packet[10:12], 16)
                                    spc = int(packet[5], 16)
                                
                                    for id in range(1, spc + 1):
                                        discovery_name = '{}_{:0>2d}_{:0>2d}'.format(name, rid, id)

                                        if discovery_name not in DISCOVERY_LIST:
                                            DISCOVERY_LIST.append(discovery_name)
                                    
                                            for payload_template in DISCOVERY_PAYLOAD[name]:
                                                payload = payload_template.copy()
                                                payload['~'] = payload['~'].format(rid, id)
                                                payload['name'] = payload['name'].format(rid, id)
                                   
                                                # 장치 등록 후 DISCOVERY_DELAY초 후에 State 업데이트
                                                await mqtt_discovery(payload)
                                                await asyncio.sleep(DISCOVERY_DELAY)  
                                    
                                        # BIT0: 대기전력 On/Off, BIT1: 자동모드 On/Off
                                        # 위와 같지만 일단 on-off 여부만 판단
                                        #asis onoff = 'ON' if int(packet[7 + 6 * id], 16) > 0 else 'OFF'
                                        #asis autoonoff = 'ON' if int(packet[6 + 6 * id], 16) > 0 else 'OFF'
                                        #asis power_num = '{:.2f}'.format(int(packet[8 + 6 * id: 12 + 6 * id], 16) / 100)

                                        #yh 대기전력 1개당 1개의 전문으로 상태가 올라와서 for 구조가 필요없지만 변경최소화를 위해 유지함
                                        if id == spc:
                                            onoff = 'ON' if int(packet[12], 16) > 0 else 'OFF'
                                            autoonoff = 'ON' if int(packet[12], 16) > 7 else 'OFF'
                                            power_num = '{:.2f}'.format(int(packet[13], 16)*1000 + int(packet[14], 16) * 100 + int(packet[15], 16) * 10 + int(packet[16], 16) + int(packet[17], 16) * 0.1)
                                            
                                            await update_state(name, 'power', rid, id, onoff)
                                            await update_state(name, 'auto', rid, id, onoff)
                                            await update_state(name, 'current', rid, id, power_num)
                                    
                                        # 직전 처리 State 패킷은 저장
                                        MSG_CACHE[packet[0:10]] = packet[10:]
                                else:
                                    # ROOM ID
                                    #yh rid = int(packet[5], 16)
                                    rid = int(packet[4], 16)
                                    # ROOM의 plug 갯수
                                    #yh sid = int(packet[10:12], 16) 
                                    sid = int(packet[5], 16)
                                
                                    #yh onoff = 'ON' if int(packet[13], 16) > 0 else 'OFF'
                                    onoff = 'ON' if int(packet[12], 16) > 0 else 'OFF'
                                    
                                    await update_state(name, 'power', rid, id, onoff)
                                    
                            # yh plug 응답시 ACT, STATE 두개가 동시에 와서 이건필요없음
                            elif name == 'plug' and ACK_PACKET:
                                #log('[YH] ->> TEST(plug ack) : [{}]'.format(packet))
                                if ACK_PACKET:
                                    # ROOM ID
                                    #yh rid = int(packet[5], 16)
                                    rid = int(packet[4], 16)
                                    # ROOM의 plug 갯수
                                    #yh spc = int(packet[10:12], 16)
                                    spc = int(packet[5], 16)
                                
                                    onoff = 'ON' if int(packet[12], 16) > 0 else 'OFF'
                                    autoonoff = 'ON' if int(packet[12], 16) > 7 else 'OFF'
                                    
                                    #await update_state(name, 'power', rid, spc, onoff)
                                    #await update_state(name, 'auto', rid, spc, onoff)
                                
                            elif name == 'gasvalve':
                                # Gas Value는 하나라서 강제 설정
                                rid = 1
                                # Gas Value는 하나라서 강제 설정
                                spc = 1 
                                
                                discovery_name = '{}_{:0>2d}_{:0>2d}'.format(name, rid, spc)
                                    
                                if discovery_name not in DISCOVERY_LIST:
                                    DISCOVERY_LIST.append(discovery_name)
                                    
                                    payload = DISCOVERY_PAYLOAD[name][0].copy()
                                    payload['~'] = payload['~'].format(rid, spc)
                                    payload['name'] = payload['name'].format(rid, spc)
                                   
                                    # 장치 등록 후 DISCOVERY_DELAY초 후에 State 업데이트
                                    await mqtt_discovery(payload)
                                    await asyncio.sleep(DISCOVERY_DELAY)                                

                                onoff = 'ON' if int(packet[12:14], 16) == 1 else 'OFF'
                                        
                                await update_state(name, 'power', rid, spc, onoff)
                                
                                # 직전 처리 State 패킷은 저장
                                if STATE_PACKET:
                                    MSG_CACHE[packet[0:10]] = packet[10:]

                            #yh 환풍기 추가
                            elif name == 'fan' and STATE_PACKET:
                                rid = 1
                                fid = 1
                            
                                discovery_name = '{}_{:0>2d}_{:0>2d}'.format(name, rid, fid)
                                if discovery_name not in DISCOVERY_LIST:
                                    DISCOVERY_LIST.append(discovery_name)
                            
                                    payload = DISCOVERY_PAYLOAD[name][0].copy()
                                    payload['~'] = payload['~'].format(rid, fid)
                                    payload['name'] = payload['name'].format(rid, fid)

                                    # 장치 등록 후 DISCOVERY_DELAY초 후에 State 업데이트
                                    await mqtt_discovery(payload)
                                    await asyncio.sleep(DISCOVERY_DELAY)
                            
                                # 🔽 패킷 위치는 직접 채우세요
                                power = 'ON' if int(packet[13], 16) == 1 else 'OFF'
                            
                                spd = int(packet[14], 16)
                                percentage = { 1: 33, 2: 66, 3: 100 }.get(spd, 33)
                            
                                preset = 'energysaving' if int(packet[17], 16) == 3 else 'bypass'

                                power = 'ON'
                                percentage = 33
                                preset = 'energysaving'
                                await update_state('fan', 'state', rid, fid, power)
                                await update_state('fan', 'percentage', rid, fid, percentage)
                                await update_state('fan', 'preset_mode', rid, fid, preset)
                            
                                MSG_CACHE[packet[0:10]] = packet[10:]
                            
                            # 일괄차단기 ACK PACKET은 상태 업데이트에 반영하지 않음
                            elif name == 'batch' and STATE_PACKET:
                                # 일괄차단기는 하나라서 강제 설정
                                rid = 1
                                # 일괄차단기는 하나라서 강제 설정
                                sbc = 1
                                
                                discovery_name = '{}_{:0>2d}_{:0>2d}'.format(name, rid, sbc)
                                
                                if discovery_name not in DISCOVERY_LIST:
                                    DISCOVERY_LIST.append(discovery_name)
                                    
                                    for payload_template in DISCOVERY_PAYLOAD[name]:
                                        payload = payload_template.copy()
                                        payload['~'] = payload['~'].format(rid, sbc)
                                        payload['name'] = payload['name'].format(rid, sbc)
                                   
                                        # 장치 등록 후 DISCOVERY_DELAY초 후에 State 업데이트
                                        await mqtt_discovery(payload)
                                        await asyncio.sleep(DISCOVERY_DELAY)           

                                # 일괄 차단기는 버튼 상태 변수 업데이트
                                states = bin(int(packet[12:14], 16))[2:].zfill(8)
                                        
                                ELEVDOWN = states[2]                                        
                                ELEVUP = states[3]
                                GROUPON = states[5]
                                OUTING = states[6]
                                                                    
                                grouponoff = 'ON' if GROUPON == '1' else 'OFF'
                                outingonoff = 'ON' if OUTING == '1' else 'OFF'
                                
                                #ELEVDOWN과 ELEVUP은 직접 DEVICE_STATE에 저장
                                elevdownonoff = 'ON' if ELEVDOWN == '1' else 'OFF'
                                elevuponoff = 'ON' if ELEVUP == '1' else 'OFF'
                                DEVICE_STATE['batch_01_01elevator-up'] = elevuponoff
                                DEVICE_STATE['batch_01_01elevator-down'] = elevdownonoff
                                    
                                # 일괄 조명 및 외출 모드는 상태 업데이트
                                await update_state(name, 'group', rid, sbc, grouponoff)
                                await update_state(name, 'outing', rid, sbc, outingonoff)
                                
                                MSG_CACHE[packet[0:10]] = packet[10:]
                                                                                    
                RESIDUE = ''
                k = k + packet_length
                
            else:
                k+=1
                
    
    # MQTT Discovery로 장치 자동 등록
    async def mqtt_discovery(payload):
        intg = payload.pop('_intg')

        # MQTT 통합구성요소에 등록되기 위한 추가 내용
        payload['device'] = DISCOVERY_DEVICE
        payload['uniq_id'] = payload['name']

        # Discovery에 등록
        topic = 'homeassistant/{}/ezville_wallpad/{}/config'.format(intg, payload['name'])
        log('[INFO] 장치 등록:  {}'.format(topic))
        mqtt_client.publish(topic, json.dumps(payload))

    
    # 장치 State를 MQTT로 Publish
    async def update_state(device, state, id1, id2, value):
        nonlocal DEVICE_STATE

        deviceID = '{}_{:0>2d}_{:0>2d}'.format(device, id1, id2)
        key = deviceID + state
        
        if value != DEVICE_STATE.get(key) or FORCE_UPDATE:
            DEVICE_STATE[key] = value
            
            topic = STATE_TOPIC.format(deviceID, state)
            mqtt_client.publish(topic, value.encode())
                    
            if mqtt_log:
                log('[LOG] ->> HA : {} >> {}'.format(topic, value))

        return

    
    # HA에서 전달된 메시지 처리        
    async def HA_process(topics, value):
        nonlocal CMD_QUEUE

        device_info = topics[1].split('_')
        device = device_info[0]
        
        if mqtt_log:
            log('[LOG] HA ->> : {} -> {}'.format('/'.join(topics), value))

        if device in RS485_DEVICE:
            key = topics[1] + topics[2]
            idx = int(device_info[1])
            sid = int(device_info[2])
            cur_state = DEVICE_STATE.get(key)
            
            if value == cur_state:
                pass
            
            else:
                if device == 'thermostat':                        
                    if topics[2] == 'power':
                        if value == 'heat':
                            
                            sendcmd = checksum('F7' + RS485_DEVICE[device]['power']['id'] + '1' + str(idx) + RS485_DEVICE[device]['power']['cmd'] + '01010000')
                            recvcmd = 'F7' + RS485_DEVICE[device]['power']['id'] + '1' + str(idx) + RS485_DEVICE[device]['power']['ack']
                            statcmd = [key, value]
                           
                            await CMD_QUEUE.put({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'statcmd': statcmd})
                        
                        # Thermostat는 외출 모드를 Off 모드로 연결
                        elif value == 'off':
 
                            sendcmd = checksum('F7' + RS485_DEVICE[device]['away']['id'] + '1' + str(idx) + RS485_DEVICE[device]['away']['cmd'] + '01010000')
                            recvcmd = 'F7' + RS485_DEVICE[device]['away']['id'] + '1' + str(idx) + RS485_DEVICE[device]['away']['ack']
                            statcmd = [key, value]
                           
                            await CMD_QUEUE.put({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'statcmd': statcmd})
                        
#                        elif value == 'off':
#                        
#                            sendcmd = checksum('F7' + RS485_DEVICE[device]['power']['id'] + '1' + str(idx) + RS485_DEVICE[device]['power']['cmd'] + '01000000')
#                            recvcmd = 'F7' + RS485_DEVICE[device]['power']['id'] + '1' + str(idx) + RS485_DEVICE[device]['power']['ack']
#                            statcmd = [key, value]
#                           
#                            await CMD_QUEUE.put({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'statcmd': statcmd})                    
                                               
                        if debug:
                            log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}, statcmd: {}'.format(sendcmd, recvcmd, statcmd))
                                    
                    elif topics[2] == 'setTemp':                            
                        value = int(float(value))
   
                        sendcmd = checksum('F7' + RS485_DEVICE[device]['target']['id'] + '1' + str(idx) + RS485_DEVICE[device]['target']['cmd'] + '01' + "{:02X}".format(value) + '0000')
                        recvcmd = 'F7' + RS485_DEVICE[device]['target']['id'] + '1' + str(idx) + RS485_DEVICE[device]['target']['ack']
                        statcmd = [key, str(value)]

                        await CMD_QUEUE.put({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'statcmd': statcmd})
                               
                        if debug:
                            log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}, statcmd: {}'.format(sendcmd, recvcmd, statcmd))

#                    elif device == 'Fan':
#                        if topics[2] == 'power':
#                            sendcmd = DEVICE_LISTS[device][idx].get('command' + value)
#                            recvcmd = DEVICE_LISTS[device][idx].get('state' + value) if value == 'ON' else [
#                                DEVICE_LISTS[device][idx].get('state' + value)]
#                            QUEUE.append({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'count': 0})
#                            if debug:
#                                log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}'.format(sendcmd, recvcmd))
#                        elif topics[2] == 'speed':
#                            speed_list = ['LOW', 'MEDIUM', 'HIGH']
#                            if value in speed_list:
#                                index = speed_list.index(value)
#                                sendcmd = DEVICE_LISTS[device][idx]['CHANGE'][index]
#                                recvcmd = [DEVICE_LISTS[device][idx]['stateON'][index]]
#                                QUEUE.append({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'count': 0})
#                                if debug:
#                                    log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}'.format(sendcmd, recvcmd))

                elif device == 'light':                         
                    pwr = '01' if value == 'ON' else '00'
                        
                    sendcmd = checksum('F7' + RS485_DEVICE[device]['power']['id'] + '1' + str(idx) + RS485_DEVICE[device]['power']['cmd'] + '030' + str(sid) + pwr + '000000')
                    recvcmd = 'F7' + RS485_DEVICE[device]['power']['id'] + '1' + str(idx) + RS485_DEVICE[device]['power']['ack']
                    statcmd = [key, value]
                    
                    await CMD_QUEUE.put({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'statcmd': statcmd})
                               
                    if debug:
                        log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}, statcmd: {}'.format(sendcmd, recvcmd, statcmd))
                                
                elif device == 'plug':                         
                    pwr = '01' if value == 'ON' else '00'

                    #yh sendcmd = checksum('F7' + RS485_DEVICE[device]['power']['id'] + '1' + str(idx) + RS485_DEVICE[device]['power']['cmd'] + '020' + str(sid) + pwr + '0000')
                    #yh recvcmd = 'F7' + RS485_DEVICE[device]['power']['id'] + '1' + str(idx) + RS485_DEVICE[device]['power']['ack']
                    
                    #sendcmd = checksum('F7' + RS485_DEVICE[device]['power']['id'] +  str(idx) + str(sid) + RS485_DEVICE[device]['power']['cmd'] + '01' + pwr + '0000')
                    pwr = '11' if value == 'ON' else '10'
                    sendcmd = checksum('F7' + RS485_DEVICE[device]['power']['id'] +  str(idx) + str(sid) + RS485_DEVICE[device]['power']['cmd'] + '01' + pwr + '0000')
                    recvcmd = 'F7' + RS485_DEVICE[device]['power']['id'] +  str(idx) + str(sid) + RS485_DEVICE[device]['power']['ack']
                    statcmd = [key, value]
                        
                    await CMD_QUEUE.put({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'statcmd': statcmd})
                               
                    if debug:
                        log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}, statcmd: {}'.format(sendcmd, recvcmd, statcmd))
                                
                elif device == 'gasvalve':
                    # 가스 밸브는 ON 제어를 받지 않음
                    if value == 'OFF':
                        sendcmd = checksum('F7' + RS485_DEVICE[device]['power']['id'] + '0' + str(idx) + RS485_DEVICE[device]['power']['cmd'] + '0100' + '0000')
                        recvcmd = ['F7' + RS485_DEVICE[device]['power']['id'] + '1' + str(idx) + RS485_DEVICE[device]['power']['ack']]
                        statcmd = [key, value]

                        await CMD_QUEUE.put({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'statcmd': statcmd})
                               
                        if debug:
                            log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}, statcmd: {}'.format(sendcmd, recvcmd, statcmd))
                                
                elif device == 'batch':
                    # Batch는 Elevator 및 외출/그룹 조명 버튼 상태 고려 
                    elup_state = '1' if DEVICE_STATE.get(topics[1] + 'elevator-up') == 'ON' else '0'
                    eldown_state = '1' if DEVICE_STATE.get(topics[1] + 'elevator-down') == 'ON' else '0'
                    out_state = '1' if DEVICE_STATE.get(topics[1] + 'outing') == 'ON' else '0'
                    group_state = '1' if DEVICE_STATE.get(topics[1] + 'group') == 'ON' else '0'

                    cur_state = DEVICE_STATE.get(key)

                    # 일괄 차단기는 4가지 모드로 조절               
                    if topics[2] == 'elevator-up':
                        elup_state = '1'
                    elif topics[2] == 'elevator-down':
                        eldown_state = '1'
# 그룹 조명과 외출 모드 설정은 테스트 후에 추가 구현                                                
#                    elif topics[2] == 'group':
#                        group_state = '1'
#                    elif topics[2] == 'outing':
#                        out_state = '1'
                            
                    CMD = '{:0>2X}'.format(int('00' + eldown_state + elup_state + '0' + group_state + out_state + '0', 2))
                    
                    # 일괄 차단기는 state를 변경하여 제공해서 월패드에서 조작하도록 해야함
                    # 월패드의 ACK는 무시
                    sendcmd = checksum('F7' + RS485_DEVICE[device]['state']['id'] + '0' + str(idx) + RS485_DEVICE[device]['state']['cmd'] + '0300' + CMD + '000000')
                    recvcmd = 'NULL'
                    statcmd = [key, 'NULL']

                    #EVEVATOR_CALL_DELAY초 간격으로 EVEVATOR_CALL_CNT회 전송하도록 수정 yh
                    for rid in range(1, EVEVATOR_CALL_CNT+1):
                        await CMD_QUEUE.put({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'statcmd': statcmd})
                    
                        if debug:
                            log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}, statcmd: {}'.format(sendcmd, recvcmd, statcmd))
                        await asyncio.sleep(EVEVATOR_CALL_DELAY)
  
                                                
    # HA에서 전달된 명령을 EW11 패킷으로 전송
    async def send_to_ew11(send_data):
            
        for i in range(CMD_RETRY_COUNT):
            if ew11_log:
                log('[SIGNAL] 신호 전송: {}'.format(send_data))
                        
            if comm_mode == 'mqtt':
                mqtt_client.publish(EW11_SEND_TOPIC, bytes.fromhex(send_data['sendcmd']))
            else:
                nonlocal soc
                try:
                    soc.sendall(bytes.fromhex(send_data['sendcmd']))
                except OSError:
                    soc.close()
                    soc = initiate_socket(soc)
                    soc.sendall(bytes.fromhex(send_data['sendcmd']))
            if debug:                     
                log('[DEBUG] Iter. No.: ' + str(i + 1) + ', Target: ' + send_data['statcmd'][1] + ', Current: ' + DEVICE_STATE.get(send_data['statcmd'][0]))
             
            # Ack나 State 업데이트가 불가한 경우 한번만 명령 전송 후 Return
            if send_data['statcmd'][1] == 'NULL':
                return
      
            # FIRST_WAITTIME초는 ACK 처리를 기다림 (초당 30번 데이터가 들어오므로 ACK 못 받으면 후속 처리 시작)
            if i == 0:
                await asyncio.sleep(FIRST_WAITTIME)
            # 이후에는 정해진 간격 혹은 Random Backoff 시간 간격을 주고 ACK 확인
            else:
                if RANDOM_BACKOFF:
                    await asyncio.sleep(random.randint(0, int(CMD_INTERVAL * 1000))/1000)    
                else:
                    await asyncio.sleep(CMD_INTERVAL)
              
            if send_data['statcmd'][1] == DEVICE_STATE.get(send_data['statcmd'][0]):
                return

        if ew11_log:
            log('[SIGNAL] {}회 명령을 재전송하였으나 수행에 실패했습니다.. 다음의 Queue 삭제: {}'.format(str(CMD_RETRY_COUNT),send_data))
            return
        
                                                
    # EW11 동작 상태를 체크해서 필요시 리셋 실시
    async def ew11_health_loop():        
        while True:
            timestamp = time.time()
        
            # TIMEOUT 시간 동안 새로 받은 EW11 패킷이 없으면 재시작
            if timestamp - last_received_time > EW11_TIMEOUT:
                log('[WARNING] {} {} {}초간 신호를 받지 못했습니다. ew11 기기를 재시작합니다.'.format(timestamp, last_received_time, EW11_TIMEOUT))
                try:
                    await reset_EW11()
                    
                    restart_flag = True

                except:
                    log('[ERROR] 기기 재시작 오류! 기기 상태를 확인하세요.')
            else:
                log('[INFO] EW11 연결 상태 문제 없음')
            await asyncio.sleep(EW11_TIMEOUT)        

                                                
    # Telnet 접속하여 EW11 리셋        
    async def reset_EW11(): 
        ew11_id = config['ew11_id']
        ew11_password = config['ew11_password']
        ew11_server = config['ew11_server']

        ew11 = telnetlib.Telnet(ew11_server)

        ew11.read_until(b'login:')
        ew11.write(ew11_id.encode('utf-8') + b'\n')
        ew11.read_until(b'password:')
        ew11.write(ew11_password.encode('utf-8') + b'\n')
        ew11.write('Restart'.encode('utf-8') + b'\n')
        ew11.read_until(b'Restart..')
        
        log('[INFO] EW11 리셋 완료')
        
        # 리셋 후 60초간 Delay
        await asyncio.sleep(60)
        
    
    def initiate_socket():
        # SOCKET 통신 시작
        log('[INFO] Socket 연결을 시작합니다')
            
        retry_count = 0
        while True:
            try:
                soc = socket.socket()
                soc.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                connect_socket(soc)
                return soc
            except ConnectionRefusedError as e:
                log('[ERROR] Server에서 연결을 거부합니다. 재시도 예정 (' + str(retry_count) + '회 재시도)')
                time.sleep(1)
                retry_count += 1
                continue
             
            
    def connect_socket(socket):
        socket.connect((SOC_ADDRESS, SOC_PORT))
    

    async def serial_recv_loop():
        nonlocal soc
        nonlocal MSG_QUEUE
        
        class MSG:
            topic = ''
            payload = bytearray()
        
        msg = MSG()
        
        while True:
            try:
                # EW11 버퍼 크기만큼 데이터 받기
                DATA = soc.recv(EW11_BUFFER_SIZE)
                msg.topic = EW11_TOPIC + '/recv'
                msg.payload = DATA   
                
                MSG_QUEUE.put(msg)
                
            except OSError:
                soc.close()
                soc = initiate_socket(soc)
         
            await asyncio.sleep(SERIAL_RECV_DELAY) 
        
        
    async def state_update_loop():
        nonlocal force_target_time
        nonlocal force_stop_time
        nonlocal FORCE_UPDATE
        
        while True:
            await process_message()                    
            
            timestamp = time.time()
            
            # 정해진 시간이 지나면 FORCE 모드 발동
            if timestamp > force_target_time and not FORCE_UPDATE and FORCE_MODE:
                force_stop_time = timestamp + FORCE_DURATION
                FORCE_UPDATE = True
                log('[INFO] 상태 강제 업데이트 실시')
                
            # 정해진 시간이 지나면 FORCE 모드 종료    
            if timestamp > force_stop_time and FORCE_UPDATE and FORCE_MODE:
                force_target_time = timestamp + FORCE_PERIOD
                FORCE_UPDATE = False
                log('[INFO] 상태 강제 업데이트 종료')
                
            # STATE_LOOP_DELAY 초 대기 후 루프 진행
            await asyncio.sleep(STATE_LOOP_DELAY)
            
            
    async def command_loop():
        nonlocal CMD_QUEUE
        
        while True:
            if not CMD_QUEUE.empty():
                send_data = await CMD_QUEUE.get()
                await send_to_ew11(send_data)               
            
            # COMMAND_LOOP_DELAY 초 대기 후 루프 진행
            await asyncio.sleep(COMMAND_LOOP_DELAY)    
 

    # EW11 재실행 시 리스타트 실시
    async def restart_control():
        nonlocal mqtt_client
        nonlocal restart_flag
        nonlocal MQTT_ONLINE
        
        while True:
            if restart_flag or (not MQTT_ONLINE and ADDON_STARTED and REBOOT_CONTROL):
                if restart_flag:
                    log('[WARNING] EW11 재시작 확인')
                elif not MQTT_ONLINE and ADDON_STARTED and REBOOT_CONTROL:
                    log('[WARNING] 동작 중 MQTT Integration Offline 변경')
                
                # Asyncio Loop 획득
                loop = asyncio.get_event_loop()
                
                # MTTQ 및 socket 연결 종료
                log('[WARNING] 모든 통신 종료')
                mqtt_client.loop_stop()
                if comm_mode == 'mixed' or comm_mode == 'socket':
                    nonlocal soc
                    soc.close()
                       
                # flag 원복
                restart_flag = False
                MQTT_ONLINE = False

                # asyncio loop 종료
                log('[WARNING] asyncio loop 종료')
                loop.stop()
            
            # RESTART_CHECK_DELAY초 마다 실행
            await asyncio.sleep(RESTART_CHECK_DELAY)

        
    # MQTT 통신
    #(asis) mqtt_client = mqtt.Client('mqtt-ezville')
    from paho.mqtt.enums import CallbackAPIVersion
    mqtt_client = mqtt.Client(CallbackAPIVersion.VERSION1, 'mqtt-ezville')
    mqtt_client.username_pw_set(config['mqtt_id'], config['mqtt_password'])
    mqtt_client.on_connect = on_connect
    mqtt_client.on_disconnect = on_disconnect
    mqtt_client.on_message = on_message
    mqtt_client.connect_async(config['mqtt_server'])
    
    # asyncio loop 획득 및 EW11 오류시 재시작 task 등록
    loop = asyncio.get_event_loop()
    loop.create_task(restart_control())
        
    # Discovery 및 강제 업데이트 시간 설정
    force_target_time = time.time() + FORCE_PERIOD
    force_stop_time = force_target_time + FORCE_DURATION
    

    while True:
        # MQTT 통신 시작
        mqtt_client.loop_start()
        # MQTT Integration의 Birth/Last Will Testament를 기다림 (1초 단위)
        while not MQTT_ONLINE and REBOOT_CONTROL:
            log('[INFO] Waiting for MQTT connection')
            time.sleep(1)
        
        # socket 통신 시작       
        if comm_mode == 'mixed' or comm_mode == 'socket':
            soc = initiate_socket()  

        log('[INFO] 장치 등록 및 상태 업데이트를 시작합니다')

        tasklist = []
 
        # 필요시 Discovery 등의 지연을 위해 Delay 부여 
        time.sleep(startup_delay)      
  
        # socket 데이터 수신 loop 실행
        if comm_mode == 'socket':
            tasklist.append(loop.create_task(serial_recv_loop()))
        # EW11 패킷 기반 state 업데이트 loop 실행
        tasklist.append(loop.create_task(state_update_loop()))
        # Home Assistant 명령 실행 loop 실행
        tasklist.append(loop.create_task(command_loop()))
        # EW11 상태 체크 loop 실행
        tasklist.append(loop.create_task(ew11_health_loop()))
        
        # ADDON 정상 시작 Flag 설정
        ADDON_STARTED = True
        loop.run_forever()
        
        # 이전 task는 취소
        log('[INFO] 이전 실행 Task 종료')
        for task in tasklist:
            task.cancel()

        ADDON_STARTED = False
        
        # 주요 변수 초기화    
        MSG_QUEUE = Queue()
        CMD_QUEUE = asyncio.Queue()
        DEVICE_STATE = {}
        MSG_CACHE = {}
        DISCOVERY_LIST = []
        RESIDUE = ''


if __name__ == '__main__':
    with open(config_dir + '/options.json') as file:
        CONFIG = json.load(file)
    
    ezville_loop(CONFIG)
