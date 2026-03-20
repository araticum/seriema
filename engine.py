from typing import Dict, Any, List, Optional
from .models import Rule

def evaluate_rules(event_payload: Dict[str, Any], active_rules: List[Rule]) -> Optional[Rule]:
    """
    Avalia os campos de entrada do evento contra as regras ativas cadastradas no banco.
    Uma regra tem `condition_json` (ex: {"source": "prometheus", "severity": "CRITICAL"}).
    Retorna a primeira regra que bater com as condições (AND simples).
    """
    for rule in active_rules:
        condition = rule.condition_json
        is_match = True
        for key, value in condition.items():
            if event_payload.get(key) != value:
                is_match = False
                break
                
        if is_match:
            return rule
            
    return None
