from voluptuous import Schema, Required, Optional

CONFIG_CONTRACT = Schema([{
    Required('table_name'): str,
    Required('search_pattern'): str,
    Required('key_properties'): [str],
    Optional('search_prefix'): str,
    Optional('date_overrides'): [str],
    Optional('delimiter'): str,
    Optional('columns'): [{
        Required('name'): str,
        Optional('source_name'): str,
        Optional('value'): [str],
    }],
}])

MUNGE_CONTRACT = Schema({
    Required('columns'): [{
        Required('name'): str,
        Required('steps'): [{
            Required('method'): str,
            Required('value'): str,
            
        }]
    }]
})