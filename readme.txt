README - SCTE104Insert.py

This program takes a CSV file with data about splice timings (spliceFile.csv) and details about an encoder and creates relevant SCTE104 messages and sends them to the encoder, it also processes and displays the response

The file used may have a start time on line index[1] after the timeout(ms) data field, if this is included the program will use this as the stream start time and ignore returned encoder times. This is only used for outdated model encoders in which the time doesn't work correctly.

If loop mode is set to true, this means that if the encoder loops and goes to an earlier time, the SCTE104Insert program will redo the file and send it in every time the encoder loops.


HOW TO RUN

Run from command line:
python SCTE104Insert.py [param1] [param2] [param3] [param4] [param5]
[param1] - (String) file name for the CSV file (with or without extension)
[param2] - (String) ip address for the encoder
[param3] - (Integer) port number for the encoder
[param4] - (Boolean) Loop mode BOOLEAN value
[param5] - (String) Debug mode (optional - leave blank if no debug)