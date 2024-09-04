Uses a service account to make requests, setup GCP project, create service account, setup oauth disclosure and delegation, enable Google Drive API. TODO: ~~request pauses when hitting ratelimits.~~ none, this is the final build


--revoke to actually revoke permissions 
--limit will limit the amount of files checked to whatever number you pass with it


IF YOU ENCOUNTER A PROBLEM WHERE THE SCRIPT RETURNS 0 FILES, IT IS DUE TO THE SERVICE ACCOUNT HAVING ACCESS TO 0 FILES. ONCE YOU HAVE SHARED FILES TO THE SERVICE ACCOUNT THEY WILL SHOW UP
