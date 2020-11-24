import spacy
from constants import Constants as Cte
from query_utils import AbstractNLP

spacy_nlp = spacy.load("en_core_web_lg")


class TextNLP (AbstractNLP):
    def __init__(self, raw_text, ref_keywords_clean):
        super().__init__(raw_text)
        self.ref_keywords_clean = ref_keywords_clean

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

    def value_in_list(self, previous_errors):
        similar_error = ""
        keyword = self.get_keyword(self.clean_text)
        # USE NLP TO MATCH KEYBOARDS AND OTHER ERRORS
        nlp_error = spacy_nlp(self.clean_text)
        # Check if the current error can be grouped with any other previous error
        for previous_error in previous_errors:
            if previous_error:
                clean_previous_error = self.preprocess_string_nlp(previous_error)
                keywords_value_list = self.get_keyword(clean_previous_error)

                if keyword == keywords_value_list:
                    similar_error = previous_error
                    break

                nlp_previous_error = spacy_nlp(clean_previous_error)  # Finding the similarity
                score = nlp_error.similarity(nlp_previous_error)

                # IF THEY ARE SIMILAR (NLP) OR THEY CONTAIN THE SAME KEYBOARD --> SAME ISSUE
                if score > Cte.THRESHOLD_NLP:
                    similar_error = previous_error
                    break

        return similar_error, keyword


def group_data(grouped_by_error, error_data, list_same_ref, constants):
    ############ ADD EXTRA INFO ############
    error_log = error_data[constants.REF_LOG]
    error_data.update({constants.REF_NUM_ERRORS: 1})

    ########### DETECT KEYWORDS ###########
    error_nlp = TextNLP(error_log, constants.KEYWORDS_ERRORS)
    previous_errors = list(grouped_by_error.keys())
    similar_error, keyword = error_nlp.value_in_list(previous_errors)
    ############ CHOOSE REFERENCE ############
    error_ref = error_log
    if not similar_error:
        if keyword:
            error_ref = keyword
        # If the error was not grouped --> add
        grouped_by_error.update({error_ref: [error_data]})
    else:
        error_ref = similar_error
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
                grouped_by_error[error_ref][idx][Cte.REF_NUM_ERRORS] += 1
                break
        if not exactly_same_error:
            grouped_by_error[error_ref].append(error_data)
    return grouped_by_error

































