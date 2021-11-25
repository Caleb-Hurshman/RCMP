# receiver.py is the receiver side of the Reliable Calvin Message Protocol
#
# Usage: python3 receiver.py port filename
#        where port is an unused port number for the receiver side,
#        and filename is the name of the output file
#
# Author: Caleb Hurshman
# Date: 10/25/2021
# Based on the Reliable Calvin Message Protocol, outlined here:
# https://docs.google.com/document/d/19--3X5IwwDtJxeIE13WgtKm-gswR6fF8bejL468gIpo/edit#heading=h.r7wm0ewuayds
########################################################################################################################

from socket import *
import sys
from icecream import ic
import struct
import random

PACKET_SIZE = 2048

# TO TOGGLE DEBUGGING, UNCOMMENT/COMMENT THE NEXT LINE
# ic.disable()


class EndOfData(Exception):
    """ Raised when the receiver reaches the end of expected file data """
    pass

class Receiver:
    """
    Initialize a receiver process
    Open a socket and bind to the given port, open a file with the given filename for writing
    """
    def __init__(self, port, filename):
        self.port = port
        self.filename = filename
        self.next_packet_expected = 0   # track the next packet number the receiver is expecting

    def start(self):
        """ establish UDP socket for receiver, open file for reading, begin execution """
        receiver_addr = ("", self.port)
        self.receiver_socket = socket(AF_INET, SOCK_DGRAM)
        self.receiver_socket.bind(receiver_addr)
        ic("Receiver bound to localhost")

        try:
            with open(self.filename, 'wb') as self.file:
                ic("File opened for reading")

                # Main loop
                while True:
                    try:
                        self.receive_packet()
                        if self.packet_num < self.next_packet_expected:     # duplicate packet received
                            ic("Received duplicate packet")
                        elif self.packet_num == self.next_packet_expected:  # packet received has next expected packet num
                            ic("Received the next expected packet")         # NOTE: future packets are dropped
                            self.next_packet_expected += 1      # update the next expected packet
                            self.write_packet()
                        if self.is_acked:
                            self.send_ack()
                    except EndOfData:
                        print("reached end of expected data")
                        break
                    except timeout:
                        print("No messages from sender, closing connection")
                        break
        finally:
            self.receiver_socket.close()


    def receive_packet(self):
        """ Receive a packet from the socket, unpack all data from the packet """
        raw_data, self.sender_addr = self.receiver_socket.recvfrom(PACKET_SIZE + 13)         # receive data from sender
        self.receiver_socket.settimeout(10)     # set timeout after 1st packet received so receiver doesn't timeout
        ic("Received a packet")                 # before sender is started

        # deconstruct raw data into a quin-tuple
        (
            self.connection_id,
            self.expected_file_size,
            self.packet_num,
            self.is_acked,
            self.payload,
        ) = struct.unpack("!3I?2048s", raw_data)

        ic(self.packet_num, self.next_packet_expected)

    def write_packet(self):
        """
        Handle the case where packet is the final packet*
        Write the received data to the output file

        *Because each struct packet is always the same size, I detect the final packet in a more creative
        way than is outlined in the protocol description
        """
        if (PACKET_SIZE * (self.packet_num + 1)) > self.expected_file_size:           # if amount of data received is larger than expected, handle final packet
            remaining_data_size = -1 * ((PACKET_SIZE * self.packet_num) - self.expected_file_size)   # calculate amount of valuable data in final packet
            remaining_data = self.payload[0:remaining_data_size]                 # slice payload to get relevant data

            ic("writing final packet")
            self.file.write(remaining_data)                     # write data from final packet, not including filler
            raise EndOfData

        ic("writing payload to file")
        self.file.write(self.payload)            # if we haven't exited yet, write entire payload to file

    def send_ack(self):
        """
        Build and send an ACK packet according to RCMP specifications
        https://docs.google.com/document/d/19--3X5IwwDtJxeIE13WgtKm-gswR6fF8bejL468gIpo/edit#heading=h.r7wm0ewuayds
        """
        ack_packet = struct.pack("!2I", self.connection_id, self.packet_num)
                
        ic(self.is_acked, "sending ack packet")
        if random.randint(1, 10) <= 8:    # don't send some of the ACKs to simulate packet loss
            self.receiver_socket.sendto(ack_packet, self.sender_addr)


# Take in commandline arguments
portnum = sys.argv[1]
filename = sys.argv[2]

rec = Receiver(int(portnum), filename)
rec.start()
