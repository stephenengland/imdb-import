from enum import IntEnum

class RelationType(IntEnum):
    PRINCIPAL = 1,
    KNOWN_FOR = 2

    @classmethod
    def convert_to_enum(enums, value):
        if not value:
            return None

        try:
            int_value = int(value)
            return RelationType(int_value)
        except ValueError:
            pass
        
        try:
            return enums[str(value).upper()]
        except KeyError:
            pass

        return None
