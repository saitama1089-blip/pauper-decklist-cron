import json
import unittest

import sync_decks as sync
from helpers import FIXTURES, widget


class WidgetParserTests(unittest.TestCase):
    def test_captured_widgets_match_expected_complete_cache_payloads(self):
        for deck_id in (8027855, 8044902):
            with self.subTest(deck_id=deck_id):
                script = (FIXTURES / f"widget-{deck_id}.js").read_text(encoding="utf-8")
                expected = json.loads((FIXTURES / f"deck-{deck_id}.json").read_text(encoding="utf-8"))
                text = sync.parse_widget_deck_text(script, deck_id)
                actual = {"deck_id": deck_id, "json_decklist": sync.process_decklist_to_json(text)}
                self.assertEqual(actual, expected)

    def test_legal_larger_mainboard_and_short_sideboard(self):
        text = sync.parse_widget_deck_text(widget(deck_text="61 Island\nsideboard\n3 Dispel"), 1)
        self.assertEqual(text, "61 Island\n\n3 Dispel")

    def test_optional_or_empty_sideboard(self):
        for source in ("60 Island", "60 Island\nsideboard"):
            with self.subTest(source=source):
                self.assertEqual(sync.parse_widget_deck_text(widget(deck_text=source), 1), "60 Island")

    def test_blank_lines_do_not_turn_mainboard_cards_into_sideboard(self):
        source = "56 Island\n\n4 Counterspell\n\nsideboard\n\n3 Dispel"
        self.assertEqual(sync.parse_widget_deck_text(widget(deck_text=source), 1),
                         "56 Island\n4 Counterspell\n\n3 Dispel")

    def test_wrong_deck_identity_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "identity"):
            sync.parse_widget_deck_text(widget(10), 1)

    def test_missing_or_malformed_widget_is_rejected(self):
        for script in ("", "<html>Just a moment</html>",
                       'document.querySelector(".widget-deck-placeholder[data-id=\'1\']");'):
            with self.subTest(script=script):
                with self.assertRaises(ValueError):
                    sync.parse_widget_deck_text(script, 1)

    def test_wrong_format_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "Pauper"):
            sync.parse_widget_deck_text(widget(format_name="modern"), 1)

    def test_missing_fields_or_conflicting_deck_fields_are_rejected(self):
        markup_variants = (
            '<input name="deck_input[format]" value="pauper">',
            '<input name="deck_input[deck]" value="60 Island">',
            '<input name="deck_input[format]" value="pauper">'
            '<input name="deck_input[deck]" value="60 Island">'
            '<input name="deck_input[deck]" value="60 Mountain">',
        )
        for markup in markup_variants:
            with self.subTest(markup=markup):
                script = 'document.querySelector(".widget-deck-placeholder[data-id=\'1\']");'
                script += f"elem.innerHTML = {json.dumps(markup)};"
                with self.assertRaises(ValueError):
                    sync.parse_widget_deck_text(script, 1)

    def test_identical_duplicate_form_fields_are_allowed(self):
        markup = '<input name="deck_input[format]" value="pauper">'
        markup += '<input name="deck_input[deck]" value="60 Island">' * 2
        script = 'document.querySelector(".widget-deck-placeholder[data-id=\'1\']");'
        script += f"elem.innerHTML = {json.dumps(markup)};"
        self.assertEqual(sync.parse_widget_deck_text(script, 1), "60 Island")

    def test_bad_quantities_sizes_and_repeated_sections_are_rejected(self):
        sources = ("59 Island", "60 Island\nsideboard\n16 Dispel", "0 Island",
                   "-60 Island", "60.0 Island", "Island", "60 Island\nsideboard\nsideboard",
                   "60 Island\nsideboard\n1", "60 Island\n1 Bad\0Name")
        for source in sources:
            with self.subTest(source=source):
                with self.assertRaises(ValueError):
                    sync.parse_widget_deck_text(widget(deck_text=source), 1)

    def test_entities_unicode_and_apostrophes_are_preserved(self):
        source = "56 Island\n4 Éowyn’s Shield\nsideboard\n1 Fire // Ice"
        text = sync.parse_widget_deck_text(widget(deck_text=source), 1)
        self.assertIn("4 Éowyn's Shield", text)
        self.assertIn("1 Fire // Ice", text)

    def test_single_quoted_js_literal_is_supported(self):
        script = '''document.querySelector(".widget-deck-placeholder[data-id='1']");
        elem.innerHTML = '<input name="deck_input[format]" value="pauper"><input name="deck_input[deck]" value="60 Island">';'''
        self.assertEqual(sync.parse_widget_deck_text(script, 1), "60 Island")

    def test_returned_javascript_is_never_executed(self):
        script = widget() + '\nthrow new Error("Must never execute");'
        self.assertEqual(sync.parse_widget_deck_text(script, 1), "60 Island\n\n15 Dispel")

    def test_js_escapes_decode_without_corrupting_unicode(self):
        value = r"Éowyn\u2019s \x53hield \/ \' \$ \u{1F600} \uD83D\uDE00"
        self.assertEqual(sync._decode_js_string(value), "Éowyn’s Shield / ' $ 😀 😀")

    def test_malformed_character_escapes_are_rejected(self):
        for value in (r"\uZZZZ", r"\xZZ", r"\uD800"):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    sync._decode_js_string(value)


if __name__ == "__main__":
    unittest.main()
