import json

class JsonTreeEvaluator:
    def evaluate(self, tree, context):
        if not tree or not isinstance(tree, dict):
            return False
            
        operator = tree.get("operator", "AND").upper()
        conditions = tree.get("conditions", [])
        
        if not conditions:
            # Maybe it's a leaf node
            field = tree.get("field")
            operator_leaf = tree.get("op", "==")
            value = tree.get("value")
            
            if not field:
                return False
                
            ctx_value = context.get(field)
            if ctx_value is None:
                return False
                
            if operator_leaf == "==": return ctx_value == value
            elif operator_leaf == "!=": return ctx_value != value
            elif operator_leaf == ">": return ctx_value > value
            elif operator_leaf == "<": return ctx_value < value
            elif operator_leaf == ">=": return ctx_value >= value
            elif operator_leaf == "<=": return ctx_value <= value
            elif operator_leaf == "IN": return ctx_value in value
            elif operator_leaf == "NOT_IN": return ctx_value not in value
            
            return False

        results = [self.evaluate(c, context) for c in conditions]
        
        if operator == "AND":
            return all(results)
        elif operator == "OR":
            return any(results)
            
        return False
