class CRC16:
    @staticmethod
    def calc_next_crc_byte(crc_buff, nextbyte):
        for i in range (8):
            if( (crc_buff & 0x0001) ^ (nextbyte & 0x01) ):
                x16 = 0x8408
            else:
                x16 = 0x0000
            crc_buff = crc_buff >> 1
            crc_buff ^= x16
            nextbyte = nextbyte >> 1
        return crc_buff

    @staticmethod
    def calc_crc16(data):
        crc = 0xFFFF
        for byte in data:
            crc = CRC16.calc_next_crc_byte(crc, byte)
        return crc

# 提供的十六进制数据列表
hex_data_list = ['01', '10', '01', 'F0', '01', '70', '55', '02', '2F', '11', '00', '09', '08', '00', '64', '00', '16', '8B', 'AF', 'DC', '41', '08', '00', '6E', '00', '16', 'D8', 'AC', '9E', '41', '08', '00', 'C8', '00', '16', '87', 'E2', '7A', '42', '08', '00', '2C', '01', '16', 'D2', '86', '7B', '44', '08', '00', '91', '01', '16', 'AB', 'F2', 'A2', '40', '08', '00', 'F5', '01', '16', '91', '45', '41', '43', '08', '00', '6C', '02', '16', '00', '00', '00', '00', '08', '00', '71', '02', '16', '00', '00', '00', '00', '08', '00', '34', '03', '16', '00', '00', '00', '00']

# 将十六进制字符串列表转换为字节串
byte_data = bytearray.fromhex("".join(hex_data_list))
print(byte_data)

# 计算CRC16-CCITT校验和
checksum = CRC16.calc_crc16(byte_data)

print(f"CRC16-CCITT checksum: {checksum:#x}")