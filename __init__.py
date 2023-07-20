#from .nodes.SegGPT import segGPTNode
from .nodes import MSSqlNode
NODE_CLASS_MAPPINGS = {
#    **segGPTNode.NODE_CLASS_MAPPINGS, 
    **MSSqlNode.NODE_CLASS_MAPPINGS
}