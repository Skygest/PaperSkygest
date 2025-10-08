import re

# Patterns for detecting direct links to scientific papers
# Any link that matches these patterns will be considered a paper link, except those from excluded domains.
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
    r'ssrn\.com/abstract=\d+',        # SSRN shortened links
    
    r'proceedings\.mlr\.press/',           # Proceedings of Machine Learning Research (PMLR)
    r'proceedings\.icml\.cc/',          # International Conference on Machine Learning
    r'proceedings\.ijcai\.org/',        # International Joint Conference on AI
    r'eccv\d{4}\.org/papers/',          # European Conference on Computer Vision
    r'cvpr\d{4}\.thecvf\.com/papers/',  # Computer Vision and Pattern Recognition
    
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

    # Additional academic databases
    r'proquest\.com/docview/',          # ProQuest dissertations and research
    r'ebscohost\.com/products/research-databases/', # EBSCO research databases
    r'ingentaconnect\.com/content/',    # Ingenta academic content

    # Academic Repositories
    r'academia\.edu/\d+/',        # Academia.edu
    r'philpapers\.org/rec/',      # PhilPapers (Philosophy)
    r'hal\.science/',             # HAL (French repository)
    r'zenodo\.org/record/\d+',    # Zenodo
    r'figshare\.com/articles/',   # Figshare
    r'philsci-archive\.pitt\.edu/\d+/?', # Philosophy of science with paper ID
    r'index\.php/[a-zA-Z0-9_-]+/article/view/\d+', # Open Journal Systems (OJS) pattern
    r'/article/view/\d+/?', # Generic article view pattern
    
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
    r'arxiv\.org/',
    r'alphaxiv\.org/',                   # AlphaXiv
    r'chemrxiv\.org/',           # Chemistry
    r'eartharxiv\.org/',         # Earth Sciences
    r'psyarxiv\.com/',           # Psychology
    r'osf\.io/preprints/',       # Open Science Framework
    r'econstor\.eu/handle/',     # Economics
    # More preprint servers
    r'socarxiv\.org/',                  # Social sciences preprints
    r'engrxiv\.org/',                   # Engineering preprints
    r'sportrxiv\.org/',                 # Sports science preprints
    r'sociologicalscience\.com/articles-v\d+-\d+-\d+/?', # Sociological Science journal
    r'/articles-v\d+/', # Generic volume-based article pattern
    r'/volume/\d+/article/\d+/?', # Explicit volume/article pattern
    
    
    # Additional repository and journal platforms
    r'plos\w*\.org/[^\s<>"]+',            # PLOS journals
    r'mdpi\.com/\d+-\d+/\d+/\d+',         # MDPI journals
    r'frontiersin\.org/articles/\d+',     # Frontiers journals
    r'openreview\.net/forum\?id=',        # OpenReview (ML/AI conferences)
    r'jmlr\.org/papers/',                 # Journal of Machine Learning Research
    r'aclweb\.org/anthology/',            # ACL Anthology (Computational Linguistics)
    r'aclanthology\.org/\d{4}\.[a-z]+-[a-z]+\.\d+/?', # ACL Anthology paper URLs (year.conference-type.number)
    r'aclanthology\.org/[A-Z0-9.-]+/?', # General ACL Anthology pattern
    r'journals\.sagepub\.com/',           # SAGE Journals
    r'dl\.acm\.org/doi/',                 # ACM Digital Library (alternative format)
    r'proceedings\.neurips\.cc/',         # NeurIPS proceedings
    r'proceedings\.mlr\.press/',          # PMLR (Machine Learning Research)
    r'oup\.com/[^\s<>"]+/article/',       # Oxford University Press
    r'cambridge\.org/core/journals/',     # Cambridge University Press
    # r'dspace\.\w+\.edu/',                 # University repositories
    # r'digital\.lib\.\w+\.edu/',           # University digital libraries
    
    r'pubs\.acs\.org/doi/(?:abs|full|pdf)/10\.\d{4,}/', # ACS Publications with DOI
    r'pubs\.acs\.org/doi/10\.\d{4,}/', # ACS Publications DOI (general)
    r'acs\.org/doi/(?:abs|full|pdf)/10\.\d{4,}/', # Alternative ACS DOI format
    r'pubs\.acs\.org/journal/[a-z]+', # ACS journal homepages
    r'pubs\.acs\.org/toc/[a-z]+/\d+/\d+', # ACS table of contents
    r'acs\.org/content/acs/en/journals/', # ACS journal pages
    r'pubs\.acs\.org/action/showCitFormats\?doi=10\.\d{4,}/', # ACS citation formats
    
    # Citation patterns
    r'/doi/full/10\.\d{4,}/', # Generic DOI full pattern
    r'doi/10\.\d{4,}/',                  # DOI format without domain
    r'doi:10\.\d{4,}/',                  # DOI format without domain
    r'/doi/abs/10\.\d{4,}/', # DOI abstract page pattern
    r'/doi/pdf/10\.\d{4,}/', # DOI PDF direct link pattern
    r'/doi/(?:abs|pdf|full|epdf|pdfplus)/10\.\d{4,}/', # Common DOI URL patterns
    r'/doi/10\.\d{4,}/(?:full|pdf|abs|epub)/?', # Alternative ordering
    r'/doi/book/10\.\d{4,}/', # DOI book format
    r'/doi/chapter/10\.\d{4,}/', # DOI book chapter format
    r'/doi/proceedings/10\.\d{4,}/', # Conference proceedings
    r'/doi/article/10\.\d{4,}/', # Explicit article format
    r'/doi/[a-z]+/10\.\d{4,}/', # Generic format to catch other variations
    # r'@\w+\d{4}\w+',                     # BibTeX-style citations
    # r'\[\d+\]',                          # Numbered citation style
    # r'\(\w+ et al\.,? \d{4}\w?\)',       # Author-year citation
    # r'\w+ et al\. \(\d{4}\w?\)',         # Another author-year format
    r'annualreviews\.org/(?:doi|content/journals)/10\.\d{4,}/', # Annual Reviews
    r'annualreviews\.org/journal/[a-z]+', # Annual Reviews journal homepages
    r'annualreviews\.org/toc/[a-z]+/\d+/\d+', # Annual Reviews table of contents
    
    # GENERIC DOI PATTERN
    r'(?:https?://)?(?:dx\.)?doi\.org/10\.\d{4,}/[a-zA-Z0-9._()/-]+', # DOI URL pattern with optional https
    r'doi:10\.\d{4,}/[a-zA-Z0-9._()/-]+', # DOI prefix pattern
    # r'10\.\d{4,}/[a-zA-Z0-9._()/-]+', # Generic DOI pattern (without domain/prefix constraints) might catch as part of larger strings
    
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
    
    r'ia\.cr/\d{4}/\d{4}', # IACR Cryptology ePrint Archive shortened URLs
    r'eprint\.iacr\.org/\d{4}/\d{4}', # IACR Cryptology ePrint Archive full URLs
    
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
    # r'courtlistener\.com/',              # Court opinions and documents
    r'ssrn\.com/sol3/papers\.cfm\?.*law', # SSRN Legal Papers
    # r'repository\.law\.\w+\.edu/',       # Law school repositories
    # r'scholarship\.law\.\w+\.edu/',      # Legal scholarship repositories
    # r'casetext\.com/',                   # Legal cases
    # r'law\.cornell\.edu/',               # Legal information institute

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
    
    # Citation formats and identifiers
    r'pmid:\s?\d{8}',                  # PubMed ID references 
    r'isbn:\s?[\d-]+',                 # ISBN references (books with academic content)
]

# Enhanced content patterns to catch paper announcements
# Need two of these to count as a paper announcement
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

# PDF exclusions: do not count links from these domains as papers
PDF_EXCLUSIONS = {
    'courtlistener.com',
    'justia.com',          # Legal database, case law
    'casetext.com',        # Legal research platform, case law
    'leagle.com',          # Legal database, case law
    'pacer.gov',           # Federal court records
    'supremecourt.gov',    # Supreme Court opinions
    'uscourts.gov',        # Federal court documents
    'senate.gov/', 
    'whitehouse.gov/',
    'congress.gov/',       # US Congress documents
    # Add other domains here as needed
}

# Compile all patterns at module level
COMPILED_PAPER_PATTERNS = [re.compile(pattern, re.IGNORECASE) for pattern in paper_patterns]

COMPILED_CONTENT_PATTERNS = [re.compile(pattern, re.IGNORECASE) for pattern in content_patterns]