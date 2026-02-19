This is my portfolio website backend code.
It is simple REST API, which takes a code and returns the json data from the db.
additonally it tracks views and last accessed timestamp and limits them.

Purpouse:
use access github portfolio website only with code and data is traacked on views and access is restricted to 20 views and 30 days. Option to request access new code or renew.

User
  |
  v
Sends request with access code
(GET /<code>)
  |
  v
API receives request
  |
  v
API connects to database (codes.db)
  |
  v
Search for matching code in table
  |
  +--> If code not found
  |        |
  |        v
  |     Return error (404)
  |
  v
Validate access rules
  - Check view limit (max 20)
  - Check time limit (30 days)
  |
  +--> If limits exceeded
  |        |
  |        v
  |     Return access denied
  |
  v
Retrieve stored JSON data (payload)
  |
  v
Update:
  - views count
  - last accessed timestamp
  |
  v
Return JSON data to user