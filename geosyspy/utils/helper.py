import re

class Helper:

    @staticmethod
    def get_matched_str_from_pattern(pattern: str,
                                     text: str) -> str:
        """Returns the first occurence of the matched pattern in text.

        Args:
            pattern : A string representing the regex pattern to look for.
            text : The text to look into.

        Returns:
            A string representing the first occurence in text of the pattern.

        """
        p = re.compile(pattern)
        return p.findall(text)[0]