from arelle import Cntlr, ModelManager, ModelXbrl
from arelle.CntlrCmdLine import parseAndRun

# Path to filing XBRL instance (.xml or .xbrl file)
xbrl_file = '/path/to/instance.xbrl'

cntlr = Cntlr.Cntlr()
model_xbrl = ModelXbrl.load(cntlr, file=xbrl_file)

# Iterate through facts and find MD&A facts if tagged
for fact in model_xbrl.facts:
    if 'ManagementDiscussionAndAnalysis' in fact.concept.qname.localName:
        print(f"Concept: {fact.concept.qname}, Value: {fact.value}")

