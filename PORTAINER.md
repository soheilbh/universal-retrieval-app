# Portainer Deployment

## Important: Force Rebuild

**If deployment takes < 1 second, Portainer is NOT rebuilding** – it is restarting the old container.

To get the latest code:

1. Open your **Stack** in Portainer
2. Click **Editor** or **Re-pull and redeploy**
3. **Force rebuild:**
   - Use **Pull and redeploy** (pulls Git, then redeploys)
   - Or add a dummy change (e.g. space in `docker-compose.yml`) and redeploy
   - Or use **Build image** with **No cache** if available
4. Wait for the build to finish (typically 1–2 minutes)
5. Check the app shows **v1.1.0** under the title – that confirms the new build

## Verify Deployment

- App shows **v1.1.0** in the UI
- N-F-430214-21-07905 retrieval returns **5 sensors** (including Omega_percent)
- Metadata includes **Field Mapping** and **Per-column stats**

## GitLab Repo

- **URL:** https://git.wadacon.com/customers/np/data_retrival_n_p
- **Auth:** Username `oauth2`, Password = your GitLab token

## InfluxDB Host (Docker/Portainer)

If InfluxDB runs on the host or another container, use:
- **Host:** `host.docker.internal` (Mac/Windows) or the actual host IP
- **Port:** 8086
