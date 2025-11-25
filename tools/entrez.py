from Bio import Entrez
from datetime import datetime

Entrez.email = "your.email@example.com"  # Required by NCBI


def get_pubmed_pubdate(pmid: str) -> datetime | None:
    handle = Entrez.esummary(db="pubmed", id=pmid, retmode="xml")
    summary = Entrez.read(handle)
    handle.close()

    # summary is a list of dicts (because you can ask for multiple IDs)
    rec = summary[0]
    pubdate_str = rec.get("PubDate")
    if not pubdate_str:
        return None

    # PubDate might be like "1999 Dec", "2020 Jan 15", etc.
    try:
        return datetime.strptime(pubdate_str, "%Y %b %d")
    except ValueError:
        # Try other formats
        try:
            return datetime.strptime(pubdate_str, "%Y %b")
        except ValueError:
            try:
                return datetime.strptime(pubdate_str, "%Y")
            except ValueError:
                return None


# Example
pubmed_id = "29379200"
date = get_pubmed_pubdate(pubmed_id)
print(date)
