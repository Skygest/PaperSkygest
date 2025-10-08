from urllib.parse import unquote

def get_search_text(record) -> str:
    # Initialize all variables to empty strings or lists to avoid errors
    post_text = ""
    facet_urls = []
    mentions = []
    facet_tags = []
    standalone_tags = []
    label_values = []
    facet_url_str = ""
    mentions_str = ""
    tags_str = ""
    labels_str = ""
    
    external_uri = ""
    external_title = ""
    external_description = ""
    quoted_text = ""
    quoted_uri = ""
    images_alt_text = ""
    
    # Safely extract text from record, defaulting to empty string if not found
    post_text = record.get('text', '').lower()

    # Get URLs from facets
    for url in record.get('urls', []):
        if url:  # Check if not empty
            facet_urls.append(url.lower())
    if facet_urls:
        facet_url_str = " ".join(facet_urls)
        
    # Get mentions from facets
    for mention in record.get('mentions', []):
        if mention:  # Check if not empty
            mentions.append(mention.lower())
    if mentions:
        mentions_str = " ".join(mentions)
        
    # Get tags from facets
    for tag in record.get('facet_tags', []):
        if tag:  # Check if not empty
            facet_tags.append(tag.lower())
    
    # Get standalone tags
    if isinstance(record.get('tags'), list):
        for tag in record.get('tags', []):
            if tag:  # Check if not empty
                standalone_tags.append(tag.lower())
        # Combine all tags
    all_tags = facet_tags + standalone_tags
    if all_tags:
        tags_str = " ".join(all_tags)
    
    # Get label values
    for label in record.get('label_values', []):
        if label:  # Check if not empty
            label_values.append(label.lower())
    if label_values:
        labels_str = " ".join(label_values)

    # Safely navigate the nested embed structure
    embed_data = record.get('embed', {})
    if isinstance(embed_data, dict):
        # Check for direct external links
        external_data = embed_data.get('external', {})
        if isinstance(external_data, dict):
            external_uri = external_data.get('uri', '').lower()
            external_title = external_data.get('title', '').lower()
            external_description = external_data.get('description', '').lower()
        
        # Check for image alt texts
        alt_texts = []
        for alt_text in embed_data.get('images_alt_texts', []):
            if alt_text:  # Check if not empty
                alt_texts.append(alt_text.lower())
        if alt_texts:
            images_alt_text = " ".join(alt_texts)
        
        # Check for quoted posts
        quoted_record = embed_data.get('record', {})
        if isinstance(quoted_record, dict):
            # Get text from quoted post
            quoted_text = quoted_record.get('text', '').lower()
            # Get URI reference
            quoted_uri = quoted_record.get('uri', '').lower()
        
    try:
        if external_uri:
            external_uri = unquote(external_uri)
        if quoted_uri:
            quoted_uri = unquote(quoted_uri)
        if facet_url_str:
            facet_url_str = unquote(facet_url_str)
    except (ImportError, AttributeError):
        # If unquote isn't available, use the strings as-is
        pass

    # Combine ALL text sources for searching
    search_text = f"{post_text} {external_uri} {external_title} {external_description} {quoted_text} {quoted_uri} {facet_url_str} {mentions_str} {tags_str} {labels_str} {images_alt_text}"

    return search_text