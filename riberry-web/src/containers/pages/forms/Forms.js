import React from 'react';
import {PageContainer} from "../../../components/app/PageContainer";

export default class Forms extends React.Component {
    render() {
        return (
            <PageContainer>
                <div>Forms page {this.props.match.params.formId}</div>
            </PageContainer>
        );
    }
}