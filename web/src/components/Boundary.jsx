import { Component } from 'react';

/** A failing structure editor must not take the rest of the page with it. */
export default class Boundary extends Component {
  constructor(props) {
    super(props);
    this.state = { failed: null };
  }

  static getDerivedStateFromError(error) {
    return { failed: error };
  }

  render() {
    if (this.state.failed) {
      return <div className="sketcher sketcher-loading">
        structure editor unavailable — use the SMILES field
      </div>;
    }

    return this.props.children;
  }
}
