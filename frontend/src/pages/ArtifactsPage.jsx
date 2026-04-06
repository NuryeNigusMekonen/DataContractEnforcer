import ArtifactsPanel from "../components/ArtifactsPanel";
import useCachedPageData from "../hooks/useCachedPageData";
import { fetchArtifactsPageData } from "../services/api";

function ArtifactsPage({ refreshToken, navigate }) {
  const { data, loading, error } = useCachedPageData(fetchArtifactsPageData, {
    refreshToken,
    initialData: null,
    autoRefreshMs: 20000,
  });

  if (loading && !data) {
    return <div className="empty-state"><strong>Loading artifacts…</strong></div>;
  }

  if (error && !data) {
    return <div className="error-banner">{error}</div>;
  }

  return (
    <div className="page-stack">
      {error ? <div className="error-banner">{error}</div> : null}
      <ArtifactsPanel artifacts={data?.artifacts || {}} onNavigate={navigate} />
    </div>
  );
}

export default ArtifactsPage;
