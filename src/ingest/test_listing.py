import unittest

from ingest import parse_bls_filenames

DIRECTORY = "https://download.bls.gov/pub/time.series/pr/"

# Captured 2026-11-22 with a contact User-Agent. The hrefs are absolute.
LIVE_LISTING = """<html><head><title>download.bls.gov - /pub/time.series/pr/</title></head><body><H1>download.bls.gov - /pub/time.series/pr/</H1><hr>
<pre><A HREF="/pub/time.series/">[To Parent Directory]</A><br><br> 11/5/2026  8:30 AM          102 <A HREF="/pub/time.series/pr/pr.class">pr.class</A><br> 9/13/2022  3:52 PM          562 <A HREF="/pub/time.series/pr/pr.contacts">pr.contacts</A><br> 11/5/2026  8:30 AM      1943714 <A HREF="/pub/time.series/pr/pr.data.0.Current">pr.data.0.Current</A><br> 11/5/2026  8:30 AM      3882812 <A HREF="/pub/time.series/pr/pr.data.1.AllData">pr.data.1.AllData</A><br> 11/5/2026  8:30 AM          191 <A HREF="/pub/time.series/pr/pr.duration">pr.duration</A><br> 11/5/2026  8:30 AM           40 <A HREF="/pub/time.series/pr/pr.footnote">pr.footnote</A><br> 11/5/2026  8:30 AM          745 <A HREF="/pub/time.series/pr/pr.measure">pr.measure</A><br>  1/7/1994  2:53 PM          146 <A HREF="/pub/time.series/pr/pr.period">pr.period</A><br>11/18/2011  3:05 PM           79 <A HREF="/pub/time.series/pr/pr.seasonal">pr.seasonal</A><br> 11/5/2026  8:30 AM          263 <A HREF="/pub/time.series/pr/pr.sector">pr.sector</A><br> 11/5/2026  8:30 AM        46068 <A HREF="/pub/time.series/pr/pr.series">pr.series</A><br>11/17/2011  4:11 PM        18343 <A HREF="/pub/time.series/pr/pr.txt">pr.txt</A><br></pre><hr></body></html>"""


class ParseListingTest(unittest.TestCase):
    def test_live_listing_shape(self) -> None:
        names = parse_bls_filenames(LIVE_LISTING, DIRECTORY)
        self.assertEqual(
            names,
            [
                "pr.class",
                "pr.contacts",
                "pr.data.0.Current",
                "pr.data.1.AllData",
                "pr.duration",
                "pr.footnote",
                "pr.measure",
                "pr.period",
                "pr.seasonal",
                "pr.sector",
                "pr.series",
                "pr.txt",
            ],
        )

    def test_ignores_parent_offsite_and_nested_links(self) -> None:
        html = """
        <a href="/pub/time.series/">parent</a>
        <a href="pr.series">relative</a>
        <a href="https://example.com/pr.series">offsite</a>
        <a href="/pub/time.series/pr/extra/file.txt">nested</a>
        <a href="/pub/time.series/pr/pr.measure?C=N">sorted</a>
        """
        self.assertEqual(parse_bls_filenames(html, DIRECTORY), ["pr.series"])


if __name__ == "__main__":
    unittest.main()
