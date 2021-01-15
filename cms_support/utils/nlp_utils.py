# coding=utf-8

class AbstractNLP:
    def __init__(self, raw_text):
        self.raw_text = raw_text
        self.clean_text = self.preprocess_string_nlp(raw_text)

    def preprocess_string_nlp(self, text):
        return text.lower().strip()


class TextNLP(AbstractNLP):
    def __init__(self, raw_text, ref_keywords_clean):
        super().__init__(raw_text)
        self.ref_keywords_clean = [self.preprocess_string_nlp(keyword) for keyword in ref_keywords_clean]

    def get_keyword(self, clean_text):
        keywords_value = None
        keywords = [elem for elem in self.ref_keywords_clean if elem in clean_text]
        # We should not have more than 2 keywords in one error
        if len(keywords) > 1:
            print('ERROR! {}, {}'.format(keywords, clean_text))
            keywords_value = keywords[1]
        if keywords:
            keywords_value = keywords[0]
        return keywords_value


def group_data(grouped_by_error, error_data, list_same_ref, constant):
    ############ ADD EXTRA INFO ############
    error_log = error_data[constant.REF_LOG]
    error_data.update({constant.REF_NUM_ERRORS: 1})

    ########### DETECT KEYWORDS ###########
    error_nlp = TextNLP(error_log, constant.KEYWORDS_ERRORS)
    previous_errors = list(grouped_by_error.keys())
    keyword = error_nlp.get_keyword(error_nlp.clean_text)
    if keyword:
        error_ref = keyword
    else:
        error_ref = error_log
    ############ CHOOSE REFERENCE ############
    if keyword not in previous_errors and error_log not in previous_errors:
        # If the error was not grouped --> add
        grouped_by_error.update({error_ref: [error_data]})
    else:
        # If the error match with another --> count repetition
        exactly_same_error = False
        # EXACT SAME ERROR REPEATED --> num_errors + 1
        for idx, element in enumerate(grouped_by_error[error_ref]):
            # SAME ERROR & SAME PFN --> SAME ISSUE
            for reference in list_same_ref:
                if element[reference] == error_data[reference]:
                    exactly_same_error = True
                else:
                    exactly_same_error = False
            if exactly_same_error:
                grouped_by_error[error_ref][idx][constant.REF_NUM_ERRORS] += 1
                break
        if not exactly_same_error:
            grouped_by_error[error_ref].append(error_data)
    return grouped_by_error































