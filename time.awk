BEGIN { startTime = 0; print "appWait\tdoRequest\twriteWait\tdoWrite\twriteAck\tmongoWait\tdoRead\treadWait\tdoReply" }
/-> doRequest/                  { appWait = $1 - startTime;  startTime = $1 }
/<- doRequest/                  { doRequest = $1 - startTime; startTime = $1 }
/Entering doWrite/              { writeWait = $1 - startTime; startTime = $1 }
/Leaving doWrite/               { doWrite = $1 - startTime; startTime = $1; doWriteEnd = $1 }
/Ack with queuelen=0/           { writeAck = $1 - startTime; startTime = $1 }
/Entering doRead/               { mongoWait = $1 - doWriteEnd; startTime = $1 }
/Leaving doRead/                { doRead = $1 - startTime; startTime = $1 }
/-> doReply/                    { readWait = $1 - startTime;  startTime = $1 }
/<- doReply/                    { doReply = $1 - startTime; startTime = $1
  printf "%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n", appWait, doRequest, writeWait, doWrite, writeAck, mongoWait, doRead, readWait, doReply
}
