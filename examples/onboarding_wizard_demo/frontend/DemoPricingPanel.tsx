export async function DemoPricingPanel() {
  const snapshot = await fetch("/api/demo-pricing").then((response) => response.json());
  const { medianHomePrice } = snapshot;

  return `${snapshot.pricingScore} ${snapshot.rentIndex} ${medianHomePrice}`;
}
