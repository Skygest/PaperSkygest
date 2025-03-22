from collections import defaultdict
from atproto import models
from server.logger import logger
from server.database_dynamo import store_post, filter_events, save_events
from urllib.parse import unquote
import re
   
def contains_paper_link(search_text) -> bool:
    """
    Checks if a Bluesky post contains academic paper links or PDFs, including
    paper announcements common on social media.
    
    Args:
        record: A dictionary containing post record data with 'text' and optional 'embed' fields
        
    Returns:
        bool: True if any academic paper link or PDF is found, False otherwise
    """
    
    # Paper platform patterns for detecting direct links
    paper_patterns = [
        r'https?://[^\s<>"]+\.pdf(?:[?#][^\s<>"]*)?\b', # More precise PDF pattern
        r'https?://[^\s<>"]+/(?:download/pdf|fulltext)/\d+(?:[?#][^\s<>"]*)?\b',
        r'arxiv\.org/(?:abs|pdf)/\d{4}\.\d{4,5}',
        r'arxiv:\d{4}\.\d{4,5}',
        r'doi\.org/10\.\d{4,}/',
        r'(?:bio|med)rxiv\.org/content/10\.\d{4,}/',
        r'ncbi\.nlm\.nih\.gov/pmc/articles/PMC\d+',
        r'nber\.org/papers/w\d+',        # NBER working papers
        r'nber\.org/system/files/working_papers/w\d+/w\d+\.pdf',  # NBER PDF direct links
        r'papers\.ssrn\.com/sol3/papers\.cfm\?abstract_id=\d+',   # SSRN papers
        r'ssrn\.com/abstract=\d+'        # SSRN shortened links
        
        # Major Publishers
        r'science\.org/doi/',           # Science journals
        r'nature\.com/articles/',       # Nature journals
        r'springer\.com/article/',      # Springer
        r'sciencedirect\.com/science/article/', # Elsevier
        r'(?:www\.)?sciencedirect\.com/science/article/(?:pii/)?[A-Z0-9]+',  # ScienceDirect complete pattern
        r'wiley\.com/doi/',            # Wiley
        r'tandfonline\.com/doi/',      # Taylor & Francis
        r'ieee\.org/document/',        # IEEE
        r'acm\.org/doi/',             # ACM Digital Library
        
        # Academic Repositories
        r'academia\.edu/\d+/',        # Academia.edu
        r'philpapers\.org/rec/',      # PhilPapers (Philosophy)
        r'hal\.science/',             # HAL (French repository)
        r'zenodo\.org/record/\d+',    # Zenodo
        r'figshare\.com/articles/',   # Figshare
        
        # Generic repository patterns
        # r'\brepository\.\w+\.edu\b'
        # r'digital\.library\.\w+\.edu/',      # University digital libraries
        # r'publications\.\w+\.edu/',          # University publication sites
        r'research\.?gate\.net/',            # ResearchGate (broader pattern)
        r'academic\.oup\.com/',              # Oxford University Press journals
        # r'(?:digital|institutional)commons\.\w+\.edu/', # Digital Commons platforms
        
        # Field-Specific
        r'aps\.org/doi/',            # American Physical Society
        r'acs\.org/doi/',            # American Chemical Society
        r'ams\.org/journals/',       # American Mathematical Society
        r'asanet\.org/research/',    # American Sociological Association
        r'aeaweb\.org/articles',     # American Economic Association
        r'psycharchives\.org/',      # Psychology
        r'eric\.ed\.gov/\?id=',      # Education Resources
        r'semanticscholar\.org/paper/', # Semantic Scholar
        
        # Preprint Servers (beyond arXiv/bioRxiv)
        r'chemrxiv\.org/',           # Chemistry
        r'eartharxiv\.org/',         # Earth Sciences
        r'psyarxiv\.com/',           # Psychology
        r'osf\.io/preprints/',       # Open Science Framework
        r'econstor\.eu/handle/',     # Economics
        
        # Additional repository and journal platforms
        r'plos\w*\.org/[^\s<>"]+',            # PLOS journals
        r'mdpi\.com/\d+-\d+/\d+/\d+',         # MDPI journals
        r'frontiersin\.org/articles/\d+',     # Frontiers journals
        r'openreview\.net/forum\?id=',        # OpenReview (ML/AI conferences)
        r'jmlr\.org/papers/',                 # Journal of Machine Learning Research
        r'aclweb\.org/anthology/',            # ACL Anthology (Computational Linguistics)
        r'journals\.sagepub\.com/',           # SAGE Journals
        r'dl\.acm\.org/doi/',                 # ACM Digital Library (alternative format)
        r'proceedings\.neurips\.cc/',         # NeurIPS proceedings
        r'proceedings\.mlr\.press/',          # PMLR (Machine Learning Research)
        r'oup\.com/[^\s<>"]+/article/',       # Oxford University Press
        r'cambridge\.org/core/journals/',     # Cambridge University Press
        # r'dspace\.\w+\.edu/',                 # University repositories
        # r'digital\.lib\.\w+\.edu/',           # University digital libraries
        
        # Citation patterns
        r'doi:10\.\d{4,}/',                  # DOI format without domain
        # r'@\w+\d{4}\w+',                     # BibTeX-style citations
        # r'\[\d+\]',                          # Numbered citation style
        # r'\(\w+ et al\.,? \d{4}\w?\)',       # Author-year citation
        # r'\w+ et al\. \(\d{4}\w?\)',         # Another author-year format
                
                # Humanities and Social Sciences
        r'jstor\.org/stable/',               # JSTOR (broad humanities coverage)
        r'muse\.jhu\.edu/article/',          # Project MUSE (humanities)
        r'cairn\.info/revue',                # Cairn (French social sciences)
        r'persee\.fr/doc/',                  # Persée (French humanities archive)
        r'erudit\.org/en/journals/',         # Érudit (Canadian journals)
        r'hprints\.org/',                    # Humanities repository
        r'historycooperative\.org/',         # History
        r'ahajournals\.org/',                # American Historical Association
        r'oxfordbibliographies\.com/',       # Oxford Bibliographies
        r'shakespearequarterly\.org/',       # Literature specific
        r'linguisticsociety\.org/',          # Linguistics
        r'anthropology-news\.org/',          # Anthropology
        r'americananthro\.org/',             # American Anthropological Association
        
        r'pubmed\.ncbi\.nlm\.nih\.gov/\d+/?',
        # NCBI Books and Research Articles
        r'ncbi\.nlm\.nih\.gov/books/NBK\d+/?',         # NCBI Bookshelf (most common book ID format)
        r'ncbi\.nlm\.nih\.gov/books/n/[a-zA-Z0-9]+/',  # Alternative NCBI book format
        r'ncbi\.nlm\.nih\.gov/pmc/articles/PMC\d+/?',  # PMC articles (main research articles)
        r'ncbi\.nlm\.nih\.gov/pubmed/\d+/?',           # PubMed citations (research articles)
        r'ncbi\.nlm\.nih\.gov/research/[a-zA-Z0-9-_]+/?',  # NCBI research pages
        
        # Law and Legal Studies
        r'heinonline\.org/HOL/',             # HeinOnline legal database
        r'lawreview\.org/',                  # Law reviews
        r'jstor\.org/stable/\d+\?.*law',     # JSTOR law content
        r'courtlistener\.com/',              # Court opinions and documents
        r'ssrn\.com/sol3/papers\.cfm\?.*law', # SSRN Legal Papers
        # r'repository\.law\.\w+\.edu/',       # Law school repositories
        # r'scholarship\.law\.\w+\.edu/',      # Legal scholarship repositories
        r'casetext\.com/',                   # Legal cases
        r'law\.cornell\.edu/',               # Legal information institute

        # Medical and Health Sciences
        r'nejm\.org/doi/',                   # New England Journal of Medicine
        r'jamanetwork\.com/journals/',       # JAMA and related journals
        r'thelancet\.com/journals/',         # The Lancet journals
        r'bmj\.com/content/',                # British Medical Journal
        r'cochranelibrary\.com/',            # Cochrane Library (systematic reviews)
        r'ahajournals\.org/',                # American Heart Association journals
        r'diabetesjournals\.org/',           # Diabetes journals
        r'ascopubs\.org/',                   # American Society of Clinical Oncology
        r'who\.int/publications/',           # World Health Organization
        r'cdc\.gov/mmwr/',                   # CDC reports

        # Business, Economics and Management
        r'repec\.org/',                      # Economics papers repository
        r'econpapers\.repec\.org/paper/',    # Economics papers
        r'econbiz\.de/',                     # Economics literature
        r'hbr\.org/\d{4}/',                  # Harvard Business Review
        r'mitsloan\.mit\.edu/publication/',  # MIT Sloan Management
        r'journals\.sagepub\.com/doi/.*management', # Management journals
        r'informs\.org/Publications/',       # Operations Research
        r'aom\.org/publications/',           # Academy of Management
        r'aeaweb\.org/articles\?id=',        # American Economic Association

        # Regional and International Repositories
        # r'scielo\.\w+/',                     # SciELO (Latin America, Spain, Portugal)
        r'redalyc\.org/',                    # Latin American journals
        r'cyberleninka\.ru/article/',        # Russian scientific articles
        r'J-STAGE\.jst\.go\.jp/',            # Japanese science
        r'koreascience\.or\.kr/',            # Korean science
        r'cnki\.net/',                       # China National Knowledge Infrastructure
        r'cscd\.ac\.cn/',                    # Chinese Science Citation Database
        r'africajournals\.org/',             # African Journals
        r'ajol\.info/',                      # African Journals Online
        r'sabinet\.co\.za/',                 # South African journals

        # Arts, Music and Cultural Studies
        r'oxfordmusiconline\.com/',          # Oxford Music Online
        r'mtosmt\.org/issues/',              # Music Theory Online
        r'artsjournal\.com/',                # Arts journals
        r'tandfonline\.com/.*culture',       # Cultural studies
        r'arthistoryjournal\.org/',          # Art history
        r'getty\.edu/publications/',         # Getty publications
        r'journals\.sagepub\.com/home/msx',  # Musicology journals
        r'film-philosophy\.com/index\.php/',  # Film studies

        # Education and Library Science
        r'journals\.sagepub\.com/home/jte',  # Teacher Education
        r'aera\.net/publications/',          # American Educational Research Association
        r'tandfonline\.com/.*education',     # Education journals
        r'ed\.gov/pubsearch/',               # Education department research
        r'eric\.ed\.gov/\?id=',              # Education Resources Information Center
        r'lisr\.org/',                       # Library & Information Science Research
        r'ala\.org/tools/publications/',     # American Library Association

        # Environmental, Agriculture and Earth Sciences
        r'journals\.ametsoc\.org/',          # American Meteorological Society
        r'agupubs\.onlinelibrary\.wiley\.com/', # American Geophysical Union
        r'sciencedirect\.com/.*environment', # Environmental science
        r'int-res\.com/',                    # Inter-Research Science Publisher (marine, ecology)
        r'esa\.org/publications/',           # Ecological Society of America
        r'agronomy\.org/publications/',      # Agronomy
        r'crops\.org/publications/',         # Crop Science
        r'soils\.org/publications/',         # Soil Science
        r'forestry\.org/publications/',      # Forestry

        # Engineering
        r'asme\.org/publications/',          # American Society of Mechanical Engineers
        r'asce\.org/publications/',          # American Society of Civil Engineers
        r'aiaa\.org/publications/',          # American Institute of Aeronautics and Astronautics
        r'spe\.org/en/publications/',        # Society of Petroleum Engineers
        r'aiche\.org/resources/publications/', # American Institute of Chemical Engineers
        r'imeche\.org/publications/',        # Institution of Mechanical Engineers
        r'istructe\.org/publications/',      # Institution of Structural Engineers
        r'theiet\.org/publishing/',          # Institution of Engineering and Technology
        r'jsse\.org/(?:issue|volume)/',      # Journal of Software Systems Engineering
        
    ]
    
    # Enhanced content patterns to catch paper announcements
    content_patterns = [
        # Personal announcements and excitement
        r'(?:excited|happy|pleased) to (?:share|announce|present)',
        r'new (?:paper|preprint|work|research|study)',
        r'our (?:paper|work|research|study|findings)',
        r'(?:i|we) (?:just|recently) (?:published|uploaded|posted)',
        r'(?:check out|take a look at) (?:our|my) (?:new |latest )?(?:paper|work|research)',
        r'proud to share',
        r'thread about (?:our|my)',
        
        # Collaborative indicators
        r'with (?:my )?(?:colleagues|collaborators|co-authors)',
        r'joint work with',
        r'led by',
        r'(?:first|latest) author',
        
        # Paper status and publication indicators
        r'now (?:available|online|published|out)',
        r'(?:preprint|paper) is (?:now |finally )?(?:up|live|out)',
        r'accepted (?:at|to|in)',
        r'to appear in',
        r'forthcoming in',
        
        # Common paper-related emoji patterns
        r'📄|📝|📑|📰|🔬|🧪|🎓|📚',
        
        # Traditional academic indicators
        r'paper (?:written|published|authored) by',
        r'(?:arxiv|biorxiv|medrxiv)',
        r'research (?:paper|article|study)',
        r'journal of',
        r'proceedings of',
        r'conference on',
        r'(?:published|accepted|appeared).*?\(\d{4}\)|(?:\(\d{4}\).*?(?:journal|conference|proceedings))'
        r'et al\.',
        
        # Journal name patterns
        r'(?:journal|proceedings|transactions) of',
        r'advances in',
        r'review[s]? of',
        r'annual (?:review|conference) on',
        
        # Additional content patterns
        r'in press|in review|under review',   # Publication status
        r'(?:paper|article) titled',          # Referring to title
        r'(?:published|presenting) (?:in|at) .{5,50}?(?:journal|conference|workshop)',  # Publication venues
        r'abstract[:;]',                      # Abstract indicator
        r'(?:code|data|supplementary) (?:is )?available',  # Resource availability
        r'we (?:propose|present|introduce|demonstrate|show|describe)',  # Common academic verbs
        r'(?:paper|preprint) link',           # Direct mention of paper link
        r'(?:full|open) access',              # Access type
        r'(?:poster|talk|presentation) at',   # Conference activities
        r'(?:thesis|dissertation)',           # Academic works
        
        # Better PDF and document detection
        #r'(?<!\w)pdf(?!\w)',                  # Standalone "PDF" mention
        r'full (?:text|paper|article)',       # Full text mentions
        r'download (?:paper|article|pdf)',    # Download mentions
        

    ]
    
    # Check for explicit paper URLs
    for pattern in paper_patterns:
        if re.search(pattern, search_text):
            return True
    
    # Look for multiple academic content indicators
    matches = 0
    for pattern in content_patterns:
        if re.search(pattern, search_text):
            matches += 1
            # Return true if we find at least two academic indicators
            if matches >= 2:
                return True
    
    return False

def contains_arxiv_link(record) -> bool:
    """
    Specifically checks for arXiv links in a post.
    
    Args:
        record: A dictionary containing post record data with 'text' and optional 'embed' fields
        
    Returns:
        bool: True if an arXiv link is found, False otherwise
    """
    # Safely extract text from record
    post_text = record.get('text', '').lower()
    
    # Safely extract external URI from embed data
    external_uri = ''
    embed_data = record.get('embed', {})
    if isinstance(embed_data, dict):
        external_data = embed_data.get('external', {})
        if isinstance(external_data, dict):
            external_uri = external_data.get('uri', '').lower()
    
    # Check for arXiv links in either the post text or external URI
    return 'arxiv.org' in post_text or 'arxiv.org' in external_uri

def get_search_text(record) -> str:
    # Safely extract text from record, defaulting to empty string if not found
    post_text = record.get('text', '').lower()

    # Safely navigate the nested embed structure
    external_uri = ''
    external_title = ''
    external_description = ''
    quoted_text = ''
    quoted_uri = ''
    quoted_title = ''
    quoted_description = ''

    embed_data = record.get('embed', {})
    if isinstance(embed_data, dict):
        # Check for direct external links
        external_data = embed_data.get('external', {})
        if isinstance(external_data, dict):
            external_uri = external_data.get('uri', '').lower()
            external_title = external_data.get('title', '').lower()
            external_description = external_data.get('description', '').lower()
            
        # Check for media embeds (new section)
        media_data = embed_data.get('media', {})
        if isinstance(media_data, dict):
            media_external = media_data.get('external', {})
            if isinstance(media_external, dict):
                media_external_uri = media_external.get('uri', '').lower()
                media_external_title = media_external.get('title', '').lower()
                media_external_description = media_external.get('description', '').lower()
        
        
        # Check for quoted posts
        quoted_record = embed_data.get('record', {})
        if isinstance(quoted_record, dict):
            # Get text from quoted post
            quoted_text = quoted_record.get('text', '').lower()
            # Check for external links in quoted post
            quoted_embed = quoted_record.get('embed', {})
            if isinstance(quoted_embed, dict):
                quoted_external = quoted_embed.get('external', {})
                if isinstance(quoted_external, dict):
                    quoted_uri = quoted_external.get('uri', '').lower()
                    quoted_title = quoted_external.get('title', '').lower()
                    quoted_description = quoted_external.get('description', '').lower()

    facet_uris = []
    facets_data = record.get('facets', [])
    if isinstance(facets_data, list):
        for facet in facets_data:
            if isinstance(facet, dict):
                features = facet.get('features', [])
                if isinstance(features, list):
                    for feat in features:
                        if isinstance(feat, dict):
                            facet_uris.append(facet.get('uri', ''))

    # URL decode all URIs to handle encoded characters
    external_uri = unquote(external_uri)
    quoted_uri = unquote(quoted_uri)
    media_external_uri = unquote(media_external_uri)  # Decode the media URI too


    facet_uris = [unquote(facet_uri) for facet_uri in facet_uris]
    facet_uri_str = " ".join(facet_uris)

    # Combine ALL text sources for searching
    search_text = f"{post_text} {external_uri} {external_title}  {media_external_uri} {media_external_title} {media_external_description} {external_description} {quoted_text} {quoted_uri} {quoted_title} {quoted_description} {facet_uri_str}"
    
    return search_text

def prepare_record(record):
    """
    Transforms a Record object into a dictionary, capturing all fields we need for paper detection.
    """
    # First, carefully extract the timestamp
    created_at = None
    if hasattr(record, 'createdAt'):
        created_at = record.createdAt
    elif hasattr(record, 'created_at'):
        created_at = record.created_at
    
    if not created_at:
        print(f"Warning: Missing timestamp in record. Available attributes: {dir(record)}")
        raise ValueError("Record must have a creation timestamp")

    # Create base record dictionary
    record_dict = {
        'text': record.text if hasattr(record, 'text') else '',
        'created_at': created_at
    }

    # Handle embed data comprehensively
    if hasattr(record, 'embed'):
        record_dict['embed'] = {}
        
        # Handle external links
        if hasattr(record.embed, 'external'):
            record_dict['embed']['external'] = {
                'uri': record.embed.external.uri if hasattr(record.embed.external, 'uri') else '',
                'title': record.embed.external.title if hasattr(record.embed.external, 'title') else '',
                'description': record.embed.external.description if hasattr(record.embed.external, 'description') else ''
            }
        
        # nikhil added 2025/03/22    
        # Handle media embeds (images, videos, or embedded URLs)
        if hasattr(record.embed, 'media'):
            record_dict['embed']['media'] = {}
            
            # # For images/videos
            # if hasattr(record.embed.media, 'images'):
            #     record_dict['embed']['media']['images'] = []
            #     for image in record.embed.media.images:
            #         img_data = {}
            #         if hasattr(image, 'alt'):
            #             img_data['alt'] = image.alt
            #         if hasattr(image, 'fullsize'):
            #             img_data['fullsize'] = image.fullsize
            #         if hasattr(image, 'thumb'):
            #             img_data['thumb'] = image.thumb
            #         record_dict['embed']['media']['images'].append(img_data)
                    
            # For external media URI (the case mentioned in the email)
            if hasattr(record.embed.media, 'external'):
                record_dict['embed']['media']['external'] = {
                    'uri': record.embed.media.external.uri if hasattr(record.embed.media.external, 'uri') else '',
                    'title': record.embed.media.external.title if hasattr(record.embed.media.external, 'title') else '',
                    'description': record.embed.media.external.description if hasattr(record.embed.media.external, 'description') else ''
                }
        
        
        ## 2025/03/22 Nikhil note: this doesn't actually work, right?
        # Handle quoted posts
        if hasattr(record.embed, 'record'):
            record_dict['embed']['record'] = {
                'text': record.embed.record.text if hasattr(record.embed.record, 'text') else ''
            }
            
            # Handle embed within quoted post
            if hasattr(record.embed.record, 'embed'):
                record_dict['embed']['record']['embed'] = {}
                if hasattr(record.embed.record.embed, 'external'):
                    record_dict['embed']['record']['embed']['external'] = {
                        'uri': record.embed.record.embed.external.uri if hasattr(record.embed.record.embed.external, 'uri') else '',
                        'title': record.embed.record.embed.external.title if hasattr(record.embed.record.embed.external, 'title') else '',
                        'description': record.embed.record.embed.external.description if hasattr(record.embed.record.embed.external, 'description') else ''
                    }

    # Handle reply data
    if hasattr(record, 'reply'):
        try:
            record_dict['reply'] = {
                'root': {'uri': record.reply.root.uri} if hasattr(record.reply, 'root') else None,
                'parent': {'uri': record.reply.parent.uri} if hasattr(record.reply, 'parent') else None
            }
        except AttributeError:
            record_dict['reply'] = None

    return record_dict