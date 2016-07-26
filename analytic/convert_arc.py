#!/usr/bin/env python

import logging
import time

from concrete import Communication, Section, AnnotationMetadata, TextSpan, Sentence, Tokenization, TokenList
from concrete.services import Annotator
from thrift.protocol import TCompactProtocol
from thrift.server import TNonblockingServer
from thrift.transport import TSocket


class CommunicationHandler:
    def annotate(self, communication):
        """
        Takes in a communication after being translated with token lattices,
        returns a new communication with regular tokens of target text
        :param communication:
        :return:
        """
        documentStr = ''
        currentDocumentLength = 0
        section_list = []
        for sec in communication.sectionList:
            sectionStr = ''
            currentSectionLength = currentDocumentLength

            sentence_list = []
            for sent in sec.sentenceList:
                sentenceStr = ''
                md = AnnotationMetadata(tool="keyword_translation", timestamp=int(time.time()))
                tokList = TokenList(tokenList=sent.tokenization.lattice.cachedBestPath.tokenList)
                newTok = Tokenization(uuid=sent.tokenization.uuid, metadata=md, kind=sent.tokenization.kind,
                                      tokenList=tokList)
                currentSentenceLength = currentSectionLength

                for tok in tokList.tokenList:
                    tokText = tok.text.decode('utf8')
                    # this is the text span for the token
                    ts = TextSpan(currentSectionLength, currentSentenceLength + len(tokText))
                    tok.textSpan = ts
                    currentSentenceLength += len(tokText)
                    sentenceStr += tokText

                # this is the text span for the sentence
                ts = TextSpan(currentSectionLength, currentSectionLength + len(sentenceStr))
                currentSectionLength += len(sentenceStr)
                sectionStr += sentenceStr
                newSent = Sentence(uuid=sent.uuid, tokenization=newTok, textSpan=ts)
                sentence_list.append(newSent)

            # this is the text span for the section
            ts = TextSpan(currentDocumentLength, currentDocumentLength + len(sectionStr))
            currentDocumentLength += len(sectionStr)
            documentStr += sectionStr
            newSec = Section(sec.uuid, sentenceList=sentence_list, textSpan=ts, kind=sec.kind)
            section_list.append(newSec)

        newComm = Communication(id=communication.id,
                                text=documentStr,
                                uuid=communication.uuid,
                                type="Translated Text",
                                metadata=AnnotationMetadata(timestamp=int(time.time()), tool="stdin"),
                                sectionList=section_list,
                                entitySetList=[],
                                entityMentionSetList=[])

        return newComm


if __name__ == "__main__":
    import argparse
    import sys

    reload(sys)
    sys.setdefaultencoding('utf8')

    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--port", dest="port", type=int, default=9090)
    options = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    handler = CommunicationHandler()
    processor = Annotator.Processor(handler)
    transport = TSocket.TServerSocket(port=options.port)
    ipfactory = TCompactProtocol.TCompactProtocolFactory()
    opfactory = TCompactProtocol.TCompactProtocolFactory()

    server = TNonblockingServer.TNonblockingServer(processor, transport, ipfactory, opfactory)
    logging.info('Starting the server...')
    server.serve()
