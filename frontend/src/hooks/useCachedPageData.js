import { useEffect, useState } from "react";

function useCachedPageData(loader, {
  initialData = null,
  refreshToken = 0,
  autoRefreshMs = 15000,
} = {}) {
  const [data, setData] = useState(initialData);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    let active = true;

    async function load(force = false) {
      try {
        if (!data) {
          setLoading(true);
        }
        const payload = await loader({ force });
        if (!active) {
          return;
        }
        setData(payload);
        setError("");
      } catch (requestError) {
        if (active) {
          setError(requestError.message || "Failed to load page data.");
        }
      } finally {
        if (active) {
          setLoading(false);
        }
      }
    }

    load(false);
    const intervalId = window.setInterval(() => {
      load(true);
    }, autoRefreshMs);

    return () => {
      active = false;
      window.clearInterval(intervalId);
    };
  }, [autoRefreshMs, loader, refreshToken]);

  async function reload(force = true) {
    setLoading(true);
    try {
      const payload = await loader({ force });
      setData(payload);
      setError("");
      return payload;
    } catch (requestError) {
      setError(requestError.message || "Failed to reload page data.");
      throw requestError;
    } finally {
      setLoading(false);
    }
  }

  return {
    data,
    loading,
    error,
    reload,
  };
}

export default useCachedPageData;
